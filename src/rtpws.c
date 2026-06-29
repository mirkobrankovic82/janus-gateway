/*! \file    rtpws.c
 * \author   Lorenzo Miniero <lorenzo@meetecho.com>
 * \copyright GNU General Public License v3
 * \brief    RTP over WebSocket support
 * \details  Integration of RTP over WebSocket functionality in the
 * Janus core, as a feature that plugins can leverage.
 *
 * \ingroup protocols
 * \ref protocols
 */

#include "rtpws.h"

#include "debug.h"
#include "rtp.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>

#ifdef HAVE_WEBSOCKETS
#include <libwebsockets.h>
#include <jansson.h>
#include <errno.h>
#endif

#define RTP_WS_SID_BYTES 16
#define RTP_WS_MAX_RTP 1500

static gboolean rtp_ws_enabled = FALSE;

gboolean janus_rtp_ws_is_enabled(void) {
	return rtp_ws_enabled;
}

gboolean janus_rtp_ws_is_available(void) {
#ifdef HAVE_WEBSOCKETS
	return TRUE;
#else
	return FALSE;
#endif
}

const char *janus_rtp_ws_compile_error(void) {
	return "RTP over WebSocket requires libwebsockets: rebuild Janus with --enable-websockets";
}

#ifndef HAVE_WEBSOCKETS

int janus_rtp_ws_init(gboolean enabled, uint16_t port, const char *path, const char *public_url,
		gboolean secure, const char *cert_pem, const char *cert_key, const char *cert_pwd) {
	(void)port;
	(void)path;
	(void)public_url;
	(void)secure;
	(void)cert_pem;
	(void)cert_key;
	(void)cert_pwd;
	if(enabled) {
		JANUS_LOG(LOG_ERR, "[RTP-WS] %s\n", janus_rtp_ws_compile_error());
		return -1;
	}
	return 0;
}

void janus_rtp_ws_deinit(void) {
	rtp_ws_enabled = FALSE;
}

janus_rtp_ws_peer *janus_rtp_ws_peer_create(void *user_data,
		janus_rtp_ws_incoming_rtp_cb incoming_rtp, janus_rtp_ws_client_gone_cb client_gone,
		const char *codec_name, int sample_rate, int channels, int ptime_ms, int payload_type) {
	(void)user_data;
	(void)incoming_rtp;
	(void)client_gone;
	(void)codec_name;
	(void)sample_rate;
	(void)channels;
	(void)ptime_ms;
	(void)payload_type;
	return NULL;
}

char *janus_rtp_ws_peer_build_url(janus_rtp_ws_peer *peer) {
	(void)peer;
	return NULL;
}

int janus_rtp_ws_peer_send_rtp(janus_rtp_ws_peer *peer, const char *rtp, int len) {
	(void)peer;
	(void)rtp;
	(void)len;
	return -1;
}

void janus_rtp_ws_peer_destroy(janus_rtp_ws_peer *peer) {
	if(!peer)
		return;
	g_free(peer->session_id);
	g_free(peer);
}

#else /* HAVE_WEBSOCKETS */

static struct lws_context *rtp_ws_context = NULL;
static GThread *rtp_ws_thread = NULL;
static GThread *rtp_ws_delivery_thread = NULL;
static volatile int rtp_ws_running = 0;
static volatile int rtp_ws_delivery_running = 0;
static char rtp_ws_path[128] = "/rtp-ws";
static char rtp_ws_public_url[256] = "";
static uint16_t rtp_ws_listen_port = 8190;
static gboolean rtp_ws_listen_secure = FALSE;
static GHashTable *rtp_ws_peers_by_sid = NULL;
static janus_mutex rtp_ws_peers_mutex = JANUS_MUTEX_INITIALIZER;

typedef struct janus_rtp_ws_out_pkt {
	int len;
	unsigned char *data;
} janus_rtp_ws_out_pkt;

typedef struct janus_rtp_ws_in_pkt {
	janus_rtp_ws_peer *peer;
	int len;
	unsigned char data[RTP_WS_MAX_RTP];
} janus_rtp_ws_in_pkt;

typedef struct janus_rtp_ws_peer_extra {
	struct lws *wsi;
	janus_mutex mutex;
	GAsyncQueue *out_queue;
	unsigned char *write_buf;
	size_t write_buf_alloc;
	size_t write_pending;
	size_t write_offset;
} janus_rtp_ws_peer_extra;

static GHashTable *rtp_ws_peer_extras = NULL;
static janus_mutex rtp_ws_extras_mutex = JANUS_MUTEX_INITIALIZER;
static GAsyncQueue *rtp_ws_in_queue = NULL;

typedef struct janus_rtp_ws_connection {
	struct lws *wsi;
	janus_rtp_ws_peer *peer;
	volatile int closing;
} janus_rtp_ws_connection;

static void janus_rtp_ws_out_pkt_free(gpointer data);
static void janus_rtp_ws_in_pkt_free(gpointer data);
static void janus_rtp_ws_peer_unref(janus_rtp_ws_peer *peer);
static void janus_rtp_ws_peer_free(const janus_refcount *p_ref);

static janus_rtp_ws_peer_extra *janus_rtp_ws_extra_get(janus_rtp_ws_peer *peer) {
	if(!peer || !rtp_ws_peer_extras)
		return NULL;
	janus_mutex_lock(&rtp_ws_extras_mutex);
	janus_rtp_ws_peer_extra *extra = g_hash_table_lookup(rtp_ws_peer_extras, peer);
	janus_mutex_unlock(&rtp_ws_extras_mutex);
	return extra;
}

static janus_rtp_ws_peer_extra *janus_rtp_ws_extra_create(janus_rtp_ws_peer *peer) {
	janus_rtp_ws_peer_extra *extra = g_malloc0(sizeof(janus_rtp_ws_peer_extra));
	janus_mutex_init(&extra->mutex);
	extra->out_queue = g_async_queue_new_full((GDestroyNotify)janus_rtp_ws_out_pkt_free);
	janus_mutex_lock(&rtp_ws_extras_mutex);
	g_hash_table_insert(rtp_ws_peer_extras, peer, extra);
	janus_mutex_unlock(&rtp_ws_extras_mutex);
	return extra;
}

static void janus_rtp_ws_out_pkt_free(gpointer data) {
	janus_rtp_ws_out_pkt *pkt = data;
	if(pkt) {
		g_free(pkt->data);
		g_free(pkt);
	}
}

static void janus_rtp_ws_in_pkt_free(gpointer data) {
	janus_rtp_ws_in_pkt *pkt = data;
	if(pkt) {
		janus_rtp_ws_peer_unref(pkt->peer);
		g_free(pkt);
	}
}

static void janus_rtp_ws_extra_destroy(gpointer data) {
	janus_rtp_ws_peer_extra *extra = data;
	if(!extra)
		return;
	janus_mutex_lock(&extra->mutex);
	extra->wsi = NULL;
	if(extra->out_queue) {
		g_async_queue_unref(extra->out_queue);
		extra->out_queue = NULL;
	}
	g_free(extra->write_buf);
	janus_mutex_unlock(&extra->mutex);
	janus_mutex_destroy(&extra->mutex);
	g_free(extra);
}

static void janus_rtp_ws_request_writable(janus_rtp_ws_peer_extra *extra) {
	if(!extra || !extra->wsi || !rtp_ws_context)
		return;
	lws_callback_on_writable(extra->wsi);
	lws_cancel_service(rtp_ws_context);
}

static int janus_rtp_ws_flush_writable(janus_rtp_ws_connection *conn, struct lws *wsi) {
	if(!conn || !conn->peer)
		return 0;
	janus_rtp_ws_peer_extra *extra = janus_rtp_ws_extra_get(conn->peer);
	if(!extra)
		return 0;

	janus_mutex_lock(&extra->mutex);

	if(lws_send_pipe_choked(wsi)) {
		lws_callback_on_writable(wsi);
		janus_mutex_unlock(&extra->mutex);
		return 0;
	}

	if(extra->write_pending == 0) {
		janus_rtp_ws_out_pkt *pkt = g_async_queue_try_pop(extra->out_queue);
		if(pkt) {
			size_t total = (size_t)pkt->len;
			if(extra->write_buf_alloc < LWS_PRE + total) {
				extra->write_buf_alloc = LWS_PRE + total;
				extra->write_buf = g_realloc(extra->write_buf, extra->write_buf_alloc);
			}
			memcpy(extra->write_buf + LWS_PRE, pkt->data, total);
			extra->write_pending = total;
			extra->write_offset = LWS_PRE;
			janus_rtp_ws_out_pkt_free(pkt);
		}
	}

	if(extra->write_pending > 0) {
		int sent = lws_write(wsi, extra->write_buf + extra->write_offset,
				extra->write_pending, LWS_WRITE_BINARY);
		if(sent < 0) {
			JANUS_LOG(LOG_WARN, "[RTP-WS] Error sending RTP over WebSocket\n");
			extra->write_pending = 0;
			extra->write_offset = 0;
		} else if((size_t)sent < extra->write_pending) {
			extra->write_offset += (size_t)sent;
			extra->write_pending -= (size_t)sent;
			lws_callback_on_writable(wsi);
		} else {
			extra->write_pending = 0;
			extra->write_offset = 0;
			if(g_async_queue_length(extra->out_queue) > 0)
				lws_callback_on_writable(wsi);
		}
	}

	janus_mutex_unlock(&extra->mutex);
	return 0;
}

static janus_rtp_ws_peer *janus_rtp_ws_lookup(const char *sid) {
	if(!sid || !rtp_ws_peers_by_sid)
		return NULL;
	janus_mutex_lock(&rtp_ws_peers_mutex);
	janus_rtp_ws_peer *peer = g_hash_table_lookup(rtp_ws_peers_by_sid, sid);
	if(peer)
		janus_refcount_increase(&peer->ref);
	janus_mutex_unlock(&rtp_ws_peers_mutex);
	return peer;
}

/* Normalize libwebsockets URL-arg output (?sid=uuid) to the bare value. */
static const char *janus_rtp_ws_urlarg(struct lws *wsi, const char *name, char *buf, size_t len) {
	const char *val = NULL;

	if(!wsi || !name || !buf || len == 0)
		return NULL;
	buf[0] = '\0';

#if (LWS_LIBRARY_VERSION_MAJOR > 4) || (LWS_LIBRARY_VERSION_MAJOR == 4 && LWS_LIBRARY_VERSION_MINOR >= 3)
	{
		int n = lws_get_urlarg_by_name_safe(wsi, name, buf, (int)len);
		if(n <= 0)
			return NULL;
		return buf;
	}
#endif

	val = lws_get_urlarg_by_name(wsi, name, buf, (int)len);
	if(!val || val[0] == '\0') {
		if(buf[0] == '\0')
			return NULL;
		val = buf;
	}
	/* Some libwebsockets builds return after "name" with a leading '='. */
	if(val[0] == '=')
		val++;
	else {
		size_t nlen = strlen(name);
		if(strncmp(val, name, nlen) == 0 && val[nlen] == '=')
			val += nlen + 1;
	}
	return (val[0] != '\0') ? val : NULL;
}

static void janus_rtp_ws_send_call_info(struct lws *wsi, janus_rtp_ws_peer *peer) {
	json_t *info = json_object();
	json_object_set_new(info, "type", json_string("call_info"));
	json_object_set_new(info, "codec", json_string(peer->codec_name ? peer->codec_name : "opus"));
	json_object_set_new(info, "sample_rate", json_integer(peer->sample_rate > 0 ? peer->sample_rate : 48000));
	json_object_set_new(info, "channels", json_integer(peer->channels > 0 ? peer->channels : 1));
	json_object_set_new(info, "ptime_ms", json_integer(peer->ptime_ms > 0 ? peer->ptime_ms : 20));
	json_object_set_new(info, "payload_type", json_integer(peer->payload_type > 0 ? peer->payload_type : 100));
	char *txt = json_dumps(info, JSON_COMPACT);
	json_decref(info);
	if(!txt)
		return;
	size_t len = strlen(txt);
	unsigned char buf[LWS_PRE + 512];
	memcpy(&buf[LWS_PRE], txt, len);
	lws_write(wsi, &buf[LWS_PRE], len, LWS_WRITE_TEXT);
	free(txt);
}

static int janus_rtp_ws_callback(struct lws *wsi, enum lws_callback_reasons reason,
		void *user, void *in, size_t len) {
	janus_rtp_ws_connection *conn = (janus_rtp_ws_connection *)user;

	switch(reason) {
		case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION: {
			char sidbuf[64] = {0};
			const char *sid = janus_rtp_ws_urlarg(wsi, "sid", sidbuf, sizeof(sidbuf));
			if(!sid || sid[0] == '\0') {
				JANUS_LOG(LOG_WARN, "[RTP-WS] WebSocket upgrade rejected: missing sid query parameter\n");
				return -1;
			}
			janus_rtp_ws_peer *peer = janus_rtp_ws_lookup(sid);
			if(!peer || g_atomic_int_get(&peer->destroyed)) {
				if(peer)
					janus_rtp_ws_peer_unref(peer);
				JANUS_LOG(LOG_WARN, "[RTP-WS] WebSocket upgrade rejected: unknown sid (%s)\n", sid);
				return -1;
			}
			conn->peer = peer;
			conn->wsi = wsi;
			return 0;
		}
		case LWS_CALLBACK_ESTABLISHED:
			if(conn && conn->peer) {
				janus_rtp_ws_peer_extra *extra = janus_rtp_ws_extra_get(conn->peer);
				if(extra) {
					janus_mutex_lock(&extra->mutex);
					extra->wsi = wsi;
					janus_mutex_unlock(&extra->mutex);
					janus_rtp_ws_request_writable(extra);
				}
				janus_rtp_ws_send_call_info(wsi, conn->peer);
			}
			break;
		case LWS_CALLBACK_RECEIVE:
			if(conn && conn->peer && lws_frame_is_binary(wsi)) {
				if(len < 12 || len > RTP_WS_MAX_RTP)
					break;
				if(!janus_is_rtp((char *)in, (int)len))
					break;
				if(g_atomic_int_get(&conn->peer->destroyed))
					break;
				janus_rtp_ws_in_pkt *pkt = g_malloc(sizeof(janus_rtp_ws_in_pkt));
				pkt->peer = conn->peer;
				janus_refcount_increase(&pkt->peer->ref);
				pkt->len = (int)len;
				memcpy(pkt->data, in, len);
				g_async_queue_push(rtp_ws_in_queue, pkt);
			}
			break;
		case LWS_CALLBACK_SERVER_WRITEABLE:
			janus_rtp_ws_flush_writable(conn, wsi);
			break;
		case LWS_CALLBACK_CLOSED:
			if(conn) {
				conn->closing = 1;
				if(conn->peer) {
					janus_rtp_ws_peer_extra *extra = janus_rtp_ws_extra_get(conn->peer);
					if(extra) {
						janus_mutex_lock(&extra->mutex);
						extra->wsi = NULL;
						extra->write_pending = 0;
						extra->write_offset = 0;
						janus_mutex_unlock(&extra->mutex);
					}
					if(conn->peer->client_gone)
						conn->peer->client_gone(conn->peer);
					janus_rtp_ws_peer_unref(conn->peer);
					conn->peer = NULL;
				}
			}
			break;
		default:
			break;
	}
	return 0;
}

static struct lws_protocols rtp_ws_protocols[] = {
	{ "janus-rtp-ws", janus_rtp_ws_callback, sizeof(janus_rtp_ws_connection), 4096, 0, NULL, 0 },
	{ NULL, NULL, 0, 0, 0, NULL, 0 }
};

static void *janus_rtp_ws_service_thread(void *data) {
	(void)data;
	while(rtp_ws_running)
		lws_service(rtp_ws_context, 50);
	return NULL;
}

static void *janus_rtp_ws_delivery_thread(void *data) {
	(void)data;
	while(rtp_ws_delivery_running || g_async_queue_length(rtp_ws_in_queue) > 0) {
		janus_rtp_ws_in_pkt *pkt = g_async_queue_timeout_pop(rtp_ws_in_queue, 100 * G_TIME_SPAN_MILLISECOND);
		if(!pkt)
			continue;
		if(!g_atomic_int_get(&pkt->peer->destroyed) && pkt->peer->incoming_rtp)
			pkt->peer->incoming_rtp(pkt->peer, (char *)pkt->data, pkt->len);
		janus_rtp_ws_in_pkt_free(pkt);
	}
	return NULL;
}

int janus_rtp_ws_init(gboolean enabled, uint16_t port, const char *path, const char *public_url,
		gboolean secure, const char *cert_pem, const char *cert_key, const char *cert_pwd) {
	rtp_ws_enabled = FALSE;
	if(!enabled)
		return 0;

	if(path && *path)
		g_snprintf(rtp_ws_path, sizeof(rtp_ws_path), "%s", path);
	if(public_url && *public_url)
		g_snprintf(rtp_ws_public_url, sizeof(rtp_ws_public_url), "%s", public_url);
	rtp_ws_listen_port = port;
	rtp_ws_listen_secure = secure;

	if(rtp_ws_context)
		return 0;

	rtp_ws_peers_by_sid = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	rtp_ws_peer_extras = g_hash_table_new_full(NULL, NULL, NULL, janus_rtp_ws_extra_destroy);
	rtp_ws_in_queue = g_async_queue_new_full((GDestroyNotify)janus_rtp_ws_in_pkt_free);

	struct lws_context_creation_info info;
	memset(&info, 0, sizeof(info));
	info.port = rtp_ws_listen_port;
	info.protocols = rtp_ws_protocols;
	info.gid = -1;
	info.uid = -1;
	info.options = LWS_SERVER_OPTION_VALIDATE_UTF8;

	if(secure && cert_pem) {
		info.ssl_cert_filepath = cert_pem;
		info.ssl_private_key_filepath = cert_key ? cert_key : cert_pem;
		info.ssl_private_key_password = cert_pwd;
#if (LWS_LIBRARY_VERSION_MAJOR == 3 && LWS_LIBRARY_VERSION_MINOR >= 2) || (LWS_LIBRARY_VERSION_MAJOR > 3)
		info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT | LWS_SERVER_OPTION_FAIL_UPON_UNABLE_TO_BIND;
#elif LWS_LIBRARY_VERSION_MAJOR >= 2
		info.options |= LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
#endif
		JANUS_LOG(LOG_VERB, "[RTP-WS] WSS certificates:\n\t%s\n\t%s\n",
			cert_pem, cert_key ? cert_key : cert_pem);
	}

	rtp_ws_context = lws_create_context(&info);
	if(!rtp_ws_context) {
		JANUS_LOG(LOG_ERR, "[RTP-WS] Failed to create libwebsockets context\n");
		return -1;
	}

	rtp_ws_running = 1;
	GError *err = NULL;
	rtp_ws_thread = g_thread_try_new("rtp-ws", janus_rtp_ws_service_thread, NULL, &err);
	if(!rtp_ws_thread) {
		JANUS_LOG(LOG_ERR, "[RTP-WS] Failed to start service thread: %s\n", err ? err->message : "??");
		if(err)
			g_error_free(err);
		return -1;
	}

	rtp_ws_delivery_running = 1;
	err = NULL;
	rtp_ws_delivery_thread = g_thread_try_new("rtp-ws-in", janus_rtp_ws_delivery_thread, NULL, &err);
	if(!rtp_ws_delivery_thread) {
		JANUS_LOG(LOG_ERR, "[RTP-WS] Failed to start delivery thread: %s\n", err ? err->message : "??");
		if(err)
			g_error_free(err);
		return -1;
	}

	rtp_ws_enabled = TRUE;
	JANUS_LOG(LOG_INFO, "[RTP-WS] RTP over WebSocket enabled (%s, port %u, path %s)\n",
		secure ? "WSS" : "WS", rtp_ws_listen_port, rtp_ws_path);
	return 0;
}

void janus_rtp_ws_deinit(void) {
	rtp_ws_delivery_running = 0;
	if(rtp_ws_delivery_thread) {
		g_thread_join(rtp_ws_delivery_thread);
		rtp_ws_delivery_thread = NULL;
	}
	rtp_ws_running = 0;
	if(rtp_ws_thread) {
		g_thread_join(rtp_ws_thread);
		rtp_ws_thread = NULL;
	}
	if(rtp_ws_context) {
		lws_cancel_service(rtp_ws_context);
		lws_context_destroy(rtp_ws_context);
		rtp_ws_context = NULL;
	}
	if(rtp_ws_in_queue) {
		g_async_queue_unref(rtp_ws_in_queue);
		rtp_ws_in_queue = NULL;
	}
	if(rtp_ws_peers_by_sid) {
		g_hash_table_destroy(rtp_ws_peers_by_sid);
		rtp_ws_peers_by_sid = NULL;
	}
	if(rtp_ws_peer_extras) {
		g_hash_table_destroy(rtp_ws_peer_extras);
		rtp_ws_peer_extras = NULL;
	}
	rtp_ws_enabled = FALSE;
}

static char *janus_rtp_ws_random_sid(void) {
	char *uuid = janus_random_uuid();
	if(uuid)
		return uuid;
	char *sid = g_malloc(RTP_WS_SID_BYTES * 2 + 1);
	for(int i = 0; i < RTP_WS_SID_BYTES; i++)
		g_snprintf(sid + (i * 2), 3, "%02x", (unsigned char)g_random_int_range(0, 256));
	return sid;
}

static void janus_rtp_ws_peer_unref(janus_rtp_ws_peer *peer) {
	if(peer)
		janus_refcount_decrease(&peer->ref);
}

static void janus_rtp_ws_peer_free(const janus_refcount *p_ref) {
	janus_rtp_ws_peer *peer = janus_refcount_containerof(p_ref, janus_rtp_ws_peer, ref);
	g_free(peer->session_id);
	g_free(peer);
}

janus_rtp_ws_peer *janus_rtp_ws_peer_create(void *user_data,
		janus_rtp_ws_incoming_rtp_cb incoming_rtp, janus_rtp_ws_client_gone_cb client_gone,
		const char *codec_name, int sample_rate, int channels, int ptime_ms, int payload_type) {
	if(!rtp_ws_enabled || !rtp_ws_peers_by_sid || !incoming_rtp)
		return NULL;
	janus_rtp_ws_peer *peer = g_malloc0(sizeof(janus_rtp_ws_peer));
	peer->user_data = user_data;
	peer->incoming_rtp = incoming_rtp;
	peer->client_gone = client_gone;
	peer->codec_name = codec_name ? codec_name : "opus";
	peer->sample_rate = sample_rate > 0 ? sample_rate : 48000;
	peer->channels = channels > 0 ? channels : 1;
	peer->ptime_ms = ptime_ms > 0 ? ptime_ms : 20;
	peer->payload_type = payload_type > 0 ? payload_type : 100;
	peer->session_id = janus_rtp_ws_random_sid();
	janus_refcount_init(&peer->ref, janus_rtp_ws_peer_free);
	janus_refcount_increase(&peer->ref);
	janus_rtp_ws_extra_create(peer);
	janus_mutex_lock(&rtp_ws_peers_mutex);
	g_hash_table_insert(rtp_ws_peers_by_sid, g_strdup(peer->session_id), peer);
	janus_mutex_unlock(&rtp_ws_peers_mutex);
	return peer;
}

char *janus_rtp_ws_peer_build_url(janus_rtp_ws_peer *peer) {
	if(!peer || !peer->session_id)
		return NULL;
	if(rtp_ws_public_url[0])
		return g_strdup_printf("%s%s?sid=%s", rtp_ws_public_url, rtp_ws_path, peer->session_id);
	return g_strdup_printf("%s://127.0.0.1:%u%s?sid=%s",
		rtp_ws_listen_secure ? "wss" : "ws", rtp_ws_listen_port, rtp_ws_path, peer->session_id);
}

int janus_rtp_ws_peer_send_rtp(janus_rtp_ws_peer *peer, const char *rtp, int len) {
	if(!peer || g_atomic_int_get(&peer->destroyed))
		return -1;
	if(!rtp || len < 12 || len > RTP_WS_MAX_RTP)
		return -1;

	janus_rtp_ws_peer_extra *extra = janus_rtp_ws_extra_get(peer);
	if(!extra)
		return -1;

	janus_rtp_ws_out_pkt *pkt = g_malloc(sizeof(janus_rtp_ws_out_pkt));
	pkt->len = len;
	pkt->data = g_malloc(len);
	memcpy(pkt->data, rtp, len);

	janus_mutex_lock(&extra->mutex);
	g_async_queue_push(extra->out_queue, pkt);
	if(extra->wsi)
		janus_rtp_ws_request_writable(extra);
	janus_mutex_unlock(&extra->mutex);
	return 0;
}

void janus_rtp_ws_peer_destroy(janus_rtp_ws_peer *peer) {
	if(!peer || !g_atomic_int_compare_and_exchange(&peer->destroyed, 0, 1))
		return;
	if(peer->session_id) {
		janus_mutex_lock(&rtp_ws_peers_mutex);
		g_hash_table_remove(rtp_ws_peers_by_sid, peer->session_id);
		janus_mutex_unlock(&rtp_ws_peers_mutex);
	}
	janus_mutex_lock(&rtp_ws_extras_mutex);
	g_hash_table_remove(rtp_ws_peer_extras, peer);
	janus_mutex_unlock(&rtp_ws_extras_mutex);
	janus_rtp_ws_peer_unref(peer);
}

#endif /* HAVE_WEBSOCKETS */
