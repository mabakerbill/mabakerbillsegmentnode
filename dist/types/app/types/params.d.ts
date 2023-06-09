import type { GroupTraits, UserTraits, CoreExtraContext, EventProperties, Integrations, Timestamp } from '@segment/analytics-core';
export type { GroupTraits, UserTraits };
/**
 * A dictionary of extra context to attach to the call.
 * Note: context differs from traits because it is not attributes of the user itself.
 */
export interface ExtraContext extends CoreExtraContext {
}
/**
 * An ID associated with the user. Note: at least one of userId or anonymousId must be included.
 **/
type IdentityOptions = {
    userId: string;
    anonymousId?: string;
} | {
    userId?: string;
    anonymousId: string;
};
export type AliasParams = {
    userId: string;
    previousId: string;
    context?: ExtraContext;
    timestamp?: Timestamp;
    integrations?: Integrations;
};
export type GroupParams = {
    groupId: string;
    /**
     * Traits are pieces of information you know about a group.
     * This interface represents reserved traits that Segment has standardized.
     * @link https://segment.com/docs/connections/spec/group/#traits
     */
    traits?: GroupTraits;
    context?: ExtraContext;
    timestamp?: Timestamp;
    integrations?: Integrations;
} & IdentityOptions;
export type IdentifyParams = {
    /**
     * Traits are pieces of information you know about a group.
     * This interface represents reserved traits that Segment has standardized.
     * @link https://segment.com/docs/connections/spec/group/#traits
     */
    traits?: UserTraits;
    context?: ExtraContext;
    timestamp?: Timestamp;
    integrations?: Integrations;
} & IdentityOptions;
export type PageParams = {
    category?: string;
    name?: string;
    properties?: EventProperties;
    timestamp?: Timestamp;
    context?: ExtraContext;
    integrations?: Integrations;
} & IdentityOptions;
export type TrackParams = {
    event: string;
    properties?: EventProperties;
    context?: ExtraContext;
    timestamp?: Timestamp;
    integrations?: Integrations;
} & IdentityOptions;
//# sourceMappingURL=params.d.ts.map