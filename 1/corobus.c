#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct data_vector {
	unsigned *data;
	size_t size;
	size_t capacity;
};

#if 1 /* Uncomment this if want to use */

/** Append @a count messages in @a data to the end of the vector. */
static void
data_vector_append_many(struct data_vector *vector,
                        const unsigned *data, size_t count) {
	if (vector->size + count > vector->capacity) {
		if (vector->capacity == 0)
			vector->capacity = 4;
		else
			vector->capacity *= 2;
		if (vector->capacity < vector->size + count)
			vector->capacity = vector->size + count;
		vector->data = realloc(vector->data,
		                       sizeof(vector->data[0]) * vector->capacity);
	}
	memcpy(&vector->data[vector->size], data, sizeof(data[0]) * count);
	vector->size += count;
}

/** Append a single message to the vector. */
static void
data_vector_append(struct data_vector *vector, unsigned data) {
	data_vector_append_many(vector, &data, 1);
}

/** Pop @a count of messages into @a data from the head of the vector. */
static void
data_vector_pop_first_many(struct data_vector *vector, unsigned *data, size_t count) {
	assert(count <= vector->size);
	memcpy(data, vector->data, sizeof(data[0]) * count);
	vector->size -= count;
	memmove(vector->data, &vector->data[count], vector->size * sizeof(vector->data[0]));
}

/** Pop a single message from the head of the vector. */
static unsigned
data_vector_pop_first(struct data_vector *vector) {
	unsigned data = 0;
	data_vector_pop_first_many(vector, &data, 1);
	return data;
}

#endif

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

#if 1 /* Uncomment this if want to use */

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue) {
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue) {
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
	                                               struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}
#endif

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	struct data_vector data;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void) {
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err) {
	global_error = err;
}

struct coro_bus *
coro_bus_new(void) {
	struct coro_bus *bus = malloc(sizeof(struct coro_bus));
	bus->channel_count = 0;
	bus->channels = NULL;
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus) {
	if (bus == NULL) return;

	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] == NULL) continue;

		free(bus->channels[i]->data.data);
		free(bus->channels[i]);
	}

	free(bus->channels);
	free(bus);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit) {
	struct coro_bus_channel *channel = malloc(sizeof(struct coro_bus_channel));
	if (!channel) {
		return -1;
	}

	channel->size_limit = size_limit;
	channel->data.size = 0;
	channel->data.capacity = 0;
	channel->data.data = NULL;
	rlist_create(&channel->send_queue.coros);
	rlist_create(&channel->recv_queue.coros);

	for (int i = 0; bus->channels && i < bus->channel_count; i++) {
		if (!bus->channels[i]) {
			bus->channels[i] = channel;
			return i;
		}
	}

	struct coro_bus_channel **storage = realloc(bus->channels,
	                                            (bus->channel_count + 1) * sizeof(struct coro_bus_channel *));
	if (!storage) {
		free(channel);
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	storage[bus->channel_count] = channel;
	bus->channels = storage;

	return bus->channel_count++;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel) {
	if (!bus || !bus->channels || channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		return;
	}

	struct coro_bus_channel *chan = bus->channels[channel];

	while (!rlist_empty(&chan->recv_queue.coros)) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		struct wakeup_entry *entry = rlist_first_entry(
			&chan->recv_queue.coros,
			struct wakeup_entry,
			base
		);
		rlist_del_entry(entry, base);
		coro_wakeup(entry->coro);
	}
	while (!rlist_empty(&chan->send_queue.coros)) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		struct wakeup_entry *entry = rlist_first_entry(
			&chan->send_queue.coros,
			struct wakeup_entry,
			base
		);
		rlist_del_entry(entry, base);
		coro_wakeup(entry->coro);
	}
	free(chan->data.data);
	free(chan);
	bus->channels[channel] = NULL;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data) {
	/*
	 * Try sending in a loop, until success. If error, then
	 * check which one is that. If 'wouldblock', then suspend
	 * this coroutine and try again when woken up.
	 *
	 * If see the channel has space, then wakeup the first
	 * coro in the send-queue. That is needed so when there is
	 * enough space for many messages, and many coroutines are
	 * waiting, they would then wake each other up one by one
	 * as lone as there is still space.
	 */
	for (;;) {
		if (bus == NULL || bus->channels == NULL || channel < 0 || channel >= bus->channel_count || !bus->channels[
			    channel]) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		struct coro_bus_channel *chan = bus->channels[channel];

		if (chan->data.size < chan->size_limit) {
			data_vector_append(&chan->data, data);
			wakeup_queue_wakeup_first(&chan->recv_queue);
			wakeup_queue_wakeup_first(&chan->send_queue);
			return 0;
		}

		wakeup_queue_suspend_this(&chan->send_queue);

		if (bus->channels[channel] != chan) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data) {
	/*
	 * Append data if has space. Otherwise 'wouldblock' error.
	 * Wakeup the first coro in the recv-queue! To let it know
	 * there is data.
	 */
	if (bus == NULL || !bus->channels || channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];
	if (chan->data.size >= chan->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	data_vector_append(&chan->data, data);
	wakeup_queue_wakeup_first(&chan->recv_queue);

	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data) {
	if (bus == NULL || bus->channels == NULL || bus->channel_count == 0 || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];
	while (chan->data.size == 0) {
		wakeup_queue_suspend_this(&chan->recv_queue);
		if (bus->channels[channel] != chan) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
	}

	*data = data_vector_pop_first(&chan->data);
	wakeup_queue_wakeup_first(&chan->send_queue);
	return 0;
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data) {
	if (bus == NULL || bus->channels == NULL || bus->channel_count == 0 || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];
	if (chan->data.size == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}


	*data = data_vector_pop_first(&chan->data);
	wakeup_queue_wakeup_first(&chan->send_queue);
	return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data) {
	while (true) {
		if (bus == NULL || bus->channels == NULL || bus->channel_count == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		bool all_ready = true;
		bool any_channel_exists = false;

		for (int i = 0; i < bus->channel_count; i++) {
			const struct coro_bus_channel *chan = bus->channels[i];
			if (chan) {
				any_channel_exists = true;
				break;
			}
		}

		if (!any_channel_exists) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		for (int i = 0; i < bus->channel_count; i++) {
			const struct coro_bus_channel *chan = bus->channels[i];
			if (!chan) continue;

			if (chan->data.size >= chan->size_limit && rlist_empty(&chan->recv_queue.coros)) {
				all_ready = false;
				break;
			}
		}

		if (all_ready) {
			for (int i = 0; i < bus->channel_count; i++) {
				struct coro_bus_channel *chan = bus->channels[i];
				if (!chan)
					continue;
				data_vector_append(&chan->data, data);
				wakeup_queue_wakeup_first(&chan->recv_queue);
			}
			return 0;
		}

		bool suspended = false;
		for (int i = 0; i < bus->channel_count; i++) {
			struct coro_bus_channel *chan = bus->channels[i];
			if (!chan)
				continue;
			if (chan->data.size >= chan->size_limit && rlist_empty(&chan->recv_queue.coros)) {
				wakeup_queue_suspend_this(&chan->send_queue);
				suspended = true;
			}
		}

		if (!suspended) {
			coro_yield();
		}
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data) {
	while (true) {
		if (bus == NULL || bus->channels == NULL || bus->channel_count == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		bool any_full = false;
		bool any_channel_exists = false;

		for (int i = 0; i < bus->channel_count; i++) {
			const struct coro_bus_channel *chan = bus->channels[i];
			if (chan) {
				any_channel_exists = true;
				break;
			}
		}

		if (!any_channel_exists) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		for (int i = 0; i < bus->channel_count; i++) {
			struct coro_bus_channel *chan = bus->channels[i];
			if (!chan) continue;

			if (chan->data.size >= chan->size_limit) {
				any_full = true;
				break;
			}
		}

		if (!any_full) {
			for (int i = 0; i < bus->channel_count; i++) {
				struct coro_bus_channel *chan = bus->channels[i];
				if (!chan)
					continue;

				data_vector_append(&chan->data, data);
				wakeup_queue_wakeup_first(&chan->recv_queue);
			}
			return 0;
		}

		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count) {
	if (bus == NULL || bus->channels == NULL || data == NULL ||
	    channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];
	unsigned sent = 0;

	for (;;) {
		if (bus->channels[channel] != chan || chan == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		unsigned space = chan->size_limit - chan->data.size;
		if (space == 0) {
			wakeup_queue_suspend_this(&chan->send_queue);

			if (bus->channels[channel] != chan || chan == NULL) {
				coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
				return -1;
			}
			continue;
		}

		unsigned to_send = (count - sent < space) ? (count - sent) : space;

		for (unsigned i = 0; i < to_send; i++) {
			data_vector_append(&chan->data, data[sent + i]);
			wakeup_queue_wakeup_first(&chan->recv_queue);
		}

		wakeup_queue_wakeup_first(&chan->send_queue);
		sent += to_send;

		return sent;
	}
}


int
coro_bus_try_send_v(struct coro_bus *bus, int channel,
                    const unsigned *data, unsigned count) {
	if (bus == NULL || bus->channels == NULL || data == NULL ||
	    channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];

	// Сколько свободного места?
	unsigned space = chan->size_limit - chan->data.size;

	// Если канал полностью заполнен
	if (space == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	// Отправим столько, сколько влезет
	unsigned to_send = (count < space) ? count : space;

	for (unsigned i = 0; i < to_send; i++) {
		data_vector_append(&chan->data, data[i]);
		wakeup_queue_wakeup_first(&chan->recv_queue);
	}

	// Разбудим других отправителей, возможно, теперь есть место
	wakeup_queue_wakeup_first(&chan->send_queue);

	return to_send;
}


int coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity) {
	if (bus == NULL || bus->channels == NULL || data == NULL ||
	    channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	int recv_count;
	while ((recv_count = coro_bus_try_recv_v(bus, channel, data, capacity)) < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			wakeup_queue_suspend_this(&bus->channels[channel]->recv_queue);
			continue;
		}

		return -1;
	}

	if (bus->channels[channel]->data.size > 0) {
		wakeup_queue_wakeup_first(&bus->channels[channel]->recv_queue);
	}

	return recv_count;
}


int
coro_bus_try_recv_v(struct coro_bus *bus, int channel,
                    unsigned *data, unsigned capacity) {
	if (bus == NULL || bus->channels == NULL || data == NULL ||
	    channel < 0 || channel >= bus->channel_count || !bus->channels[channel]) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	struct coro_bus_channel *chan = bus->channels[channel];

	if (chan->data.size == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	unsigned recv_count = (capacity < chan->data.size) ? capacity : chan->data.size;

	for (unsigned i = 0; i < recv_count; i++) {
		data[i] = data_vector_pop_first(&chan->data);
	}

	wakeup_queue_wakeup_first(&chan->send_queue);

	return recv_count;
}


#endif
