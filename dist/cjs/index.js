"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Context = exports.Analytics = void 0;
var analytics_node_1 = require("./app/analytics-node");
Object.defineProperty(exports, "Analytics", { enumerable: true, get: function () { return analytics_node_1.Analytics; } });
var context_1 = require("./app/context");
Object.defineProperty(exports, "Context", { enumerable: true, get: function () { return context_1.Context; } });
// export Analytics as both a named export and a default export (for backwards-compat. reasons)
const analytics_node_2 = require("./app/analytics-node");
exports.default = analytics_node_2.Analytics;
//# sourceMappingURL=index.js.map