"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Analytics = void 0;
const analytics_core_1 = require("@segment/analytics-core");
const settings_1 = require("./settings");
const version_1 = require("../generated/version");
const segmentio_1 = require("../plugins/segmentio");
const event_factory_1 = require("./event-factory");
const dispatch_emit_1 = require("./dispatch-emit");
const emitter_1 = require("./emitter");
const context_1 = require("./context");
const event_queue_1 = require("./event-queue");
class Analytics extends emitter_1.NodeEmitter {
    constructor(settings) {
        super();
        this._isClosed = false;
        this._pendingEvents = 0;
        (0, settings_1.validateSettings)(settings);
        this._eventFactory = new event_factory_1.NodeEventFactory();
        this._queue = new event_queue_1.NodeEventQueue();
        const flushInterval = settings.flushInterval ?? 10000;
        this._closeAndFlushDefaultTimeout = flushInterval * 1.25; // add arbitrary multiplier in case an event is in a plugin.
        const { plugin, publisher } = (0, segmentio_1.createConfiguredNodePlugin)({
            writeKey: settings.writeKey,
            host: settings.host,
            path: settings.path,
            maxRetries: settings.maxRetries ?? 3,
            maxEventsInBatch: settings.maxEventsInBatch ?? 15,
            httpRequestTimeout: settings.httpRequestTimeout,
            flushInterval,
        }, this);
        this._publisher = publisher;
        this.ready = this.register(plugin).then(() => undefined);
        this.emit('initialize', settings);
        (0, analytics_core_1.bindAll)(this);
    }
    get VERSION() {
        return version_1.version;
    }
    /**
     * Call this method to stop collecting new events and flush all existing events.
     * This method also waits for any event method-specific callbacks to be triggered,
     * and any of their subsequent promises to be resolved/rejected.
     */
    closeAndFlush({ timeout = this._closeAndFlushDefaultTimeout, } = {}) {
        this._publisher.flushAfterClose(this._pendingEvents);
        this._isClosed = true;
        const promise = new Promise((resolve) => {
            if (!this._pendingEvents) {
                resolve();
            }
            else {
                this.once('drained', () => resolve());
            }
        });
        return timeout ? (0, analytics_core_1.pTimeout)(promise, timeout).catch(() => undefined) : promise;
    }
    _dispatch(segmentEvent, callback) {
        if (this._isClosed) {
            this.emit('call_after_close', segmentEvent);
            return undefined;
        }
        this._pendingEvents++;
        (0, dispatch_emit_1.dispatchAndEmit)(segmentEvent, this._queue, this, callback)
            .catch((ctx) => ctx)
            .finally(() => {
            this._pendingEvents--;
            if (!this._pendingEvents) {
                this.emit('drained');
            }
        });
    }
    /**
     * Combines two unassociated user identities.
     * @link https://segment.com/docs/connections/sources/catalog/libraries/server/node/#alias
     */
    alias({ userId, previousId, context, timestamp, integrations }, callback) {
        const segmentEvent = this._eventFactory.alias(userId, previousId, {
            context,
            integrations,
            timestamp,
        });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * Associates an identified user with a collective.
     *  @link https://segment.com/docs/connections/sources/catalog/libraries/server/node/#group
     */
    group({ timestamp, groupId, userId, anonymousId, traits = {}, context, integrations, }, callback) {
        const segmentEvent = this._eventFactory.group(groupId, traits, {
            context,
            anonymousId,
            userId,
            timestamp,
            integrations,
        });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * Includes a unique userId and (maybe anonymousId) and any optional traits you know about them.
     * @link https://segment.com/docs/connections/sources/catalog/libraries/server/node/#identify
     */
    identify({ userId, anonymousId, traits = {}, context, timestamp, integrations, }, callback) {
        const segmentEvent = this._eventFactory.identify(userId, traits, {
            context,
            anonymousId,
            userId,
            timestamp,
            integrations,
        });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * The page method lets you record page views on your website, along with optional extra information about the page being viewed.
     * @link https://segment.com/docs/connections/sources/catalog/libraries/server/node/#page
     */
    page({ userId, anonymousId, category, name, properties, context, timestamp, integrations, }, callback) {
        const segmentEvent = this._eventFactory.page(category ?? null, name ?? null, properties, { context, anonymousId, userId, timestamp, integrations });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * Records screen views on your app, along with optional extra information
     * about the screen viewed by the user.
     *
     * TODO: This is not documented on the segment docs ATM (for node).
     */
    screen({ userId, anonymousId, category, name, properties, context, timestamp, integrations, }, callback) {
        const segmentEvent = this._eventFactory.screen(category ?? null, name ?? null, properties, { context, anonymousId, userId, timestamp, integrations });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * Records actions your users perform.
     * @link https://segment.com/docs/connections/sources/catalog/libraries/server/node/#track
     */
    track({ userId, anonymousId, event, properties, context, timestamp, integrations, }, callback) {
        const segmentEvent = this._eventFactory.track(event, properties, {
            context,
            userId,
            anonymousId,
            timestamp,
            integrations,
        });
        this._dispatch(segmentEvent, callback);
    }
    /**
     * Registers one or more plugins to augment Analytics functionality.
     * @param plugins
     */
    register(...plugins) {
        return this._queue.criticalTasks.run(async () => {
            const ctx = context_1.Context.system();
            const registrations = plugins.map((xt) => this._queue.register(ctx, xt, this));
            await Promise.all(registrations);
            this.emit('register', plugins.map((el) => el.name));
        });
    }
    /**
     * Deregisters one or more plugins based on their names.
     * @param pluginNames - The names of one or more plugins to deregister.
     */
    async deregister(...pluginNames) {
        const ctx = context_1.Context.system();
        const deregistrations = pluginNames.map((pl) => {
            const plugin = this._queue.plugins.find((p) => p.name === pl);
            if (plugin) {
                return this._queue.deregister(ctx, plugin, this);
            }
            else {
                ctx.log('warn', `plugin ${pl} not found`);
            }
        });
        await Promise.all(deregistrations);
        this.emit('deregister', pluginNames);
    }
}
exports.Analytics = Analytics;
//# sourceMappingURL=analytics-node.js.map