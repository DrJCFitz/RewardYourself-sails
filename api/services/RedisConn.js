/**
 * Use a cache for db with entries defined as
 * portal:keys - list
 * store:keys - map
 * portal:names - map

 * portal:config:<portal key> - map
 * portal:credentials:<portal key> - map
 * portal:pagedata:link:<portal key> - map
 * portal:pagedata:name:<portal key> - map
 * portal:pagedata:reward:<portal key> - map
 *
 * status:<portal key>:<portal type> - map
 * topDeals - ordered list
 * store:names - map
 */

var redis = require('redis');

var client;

var connectToRedis = function() {
	if (process.env.REDIS_PORT_6379_TCP_ADDR === undefined
		|| process.env.REDIS_PORT_6379_TCP_PORT === undefined) {

		setTimeout(connectToRedis, 1000);
	} else {
		client = new redis.createClient({
			host: process.env.REDIS_PORT_6379_TCP_ADDR, 
			port: process.env.REDIS_PORT_6379_TCP_PORT
		});
		if (client) {
			console.log('client defined');
		} else {
			console.log('client not defined');
		}
	}
}

var retrieveCredentialsByPortalID = function(portalID, type, callback) {
	if (client === undefined) {
		setTimeout(retrieveCredentialsByPortalID,500,portalID,callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.hgetall('portal:credentials:'+portalID+':'+type, callback);		
	}
}

var retrievePortalStatusKeys = function(type, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalStatusKeys, 500, type, callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.keys('status:*:'+type, callback);
	}
}

var retrievePortalStatus = function(statusKey, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalStatusByPortalID, 500, statusKey, callback);
		connectToRedis();
	} else {
		client.hgetall(statusKey, callback);
	}
}

var retrievePortalConfigByPortalID = function(portalID, type, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalConfigByPortalID,500,portalID, callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.hgetall('portal:config:'+portalID+':'+type, callback);		
	}
}

var retrievePortalLinkDataByPortalID = function(portalID, type, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalLinkDataByPortalID,500,portalID, callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.hgetall('portal:pagedata:link:'+portalID+':'+type, callback);		
	}
}

var retrievePortalNameDataByPortalID = function(portalID, type, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalNameDataByPortalID,500,portalID, callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.hgetall('portal:pagedata:name:'+portalID+':'+type, callback);		
	}
}

var retrievePortalRewardDataByPortalID = function(portalID, type, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalRewardDataByPortalID,500,portalID, callback);
		connectToRedis();
	} else {
		if (type === undefined) {
			type = 'online';
		}
		client.hgetall('portal:pagedata:reward:'+portalID+':'+type, callback);		
	}
}

var retrievePortalKeys = function(callback) {
	if (client === undefined) {
		setTimeout(retrievePortalKeys,500, callback);
		connectToRedis();
	} else {
		client.lrange('portal:keys', 0, 50, callback);
	}
}

var retrieveStoreKeys = function(callback) {
	if (client === undefined) {
		setTimeout(retrieveStoreKeys,500, callback);
		connectToRedis();
	} else {
		client.hgetall('store:keys', callback);
	}
}

var retrievePortalNames = function(callback) {
	if (client === undefined) {
		setTimeout(retrievePortalNames,500, callback);
		connectToRedis();
	} else {
		client.hgetall('portal:names', callback);
	}
}

var retrieveStoreCounts = function(portalType, callback) {
	if (client === undefined) {
		setTimeout(retrievePortalNames,500, portalType, callback);
		connectToRedis();
	} else {
		client.hgetall('store:counts', callback);
	}
}

var retrieveStoreNames = function(callback) {
	if (client === undefined) {
		setTimeout(retrievePortalNames,500, callback);
		connectToRedis();
	} else {
		client.hgetall('store:names', callback);
	}
}

connectToRedis();

module.exports = {
	retrieveCredentials: retrieveCredentialsByPortalID,
	retrievePortalConfig: retrievePortalConfigByPortalID,
	retrievePortalLink: retrievePortalLinkDataByPortalID,
	retrievePortalName: retrievePortalNameDataByPortalID,
	retrievePortalNames: retrievePortalNames,
	retrievePortalReward: retrievePortalRewardDataByPortalID,
	retrievePortalStatusKeys: retrievePortalStatusKeys,
	retrievePortalStatus: retrievePortalStatus,
	retrievePortalKeys: retrievePortalKeys,
	retrieveStoreCounts: retrieveStoreCounts,
	retrieveStoreNames: retrieveStoreNames,
	retrieveStoreKeys: retrieveStoreKeys,
};
