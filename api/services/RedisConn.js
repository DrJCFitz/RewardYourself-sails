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

var checkTopStoreEntries = function(portalType, dateCreated, callback) {
	if (client === undefined) {
		setTimeout(checkTopStoreEntries,500, portalType, dateCreated, callback);
		connectToRedis();
	} else {
		client.keys('topstores:'+portalType+':'+dateCreated, function(err,keyData){
			if (err) {
				callback(err);
			}
			if (null !== keyData) {
				client.hgetall('topstores:'+portalType+':'+dateCreated, function(err, topStoreData){
					if (err) {
						callback(err);
					}
					callback(null, topStoreData);
				});
			} else {
				callback(null, keyData);
			}
		});
	}
}

var createTopStoreEntries = function(portalType, dateCreated, dataToWrite) {
	if (client === undefined) {
		setTimeout(createTopStoreEntries,500, portalType, dateCreated);
		connectToRedis();
	} else {
		client.keys('topstores:'+portalType+':*', function(err,keyData){
			if (err) {
				console.log('error getting top store keys');
			}
			keyData.forEach(function(key){
				//console.log('deleting key: '+key);
				client.del(key, function(err, deleted){
					console.log('delete key: '+key+':'+JSON.stringify(deleted));
				});
			});
			//console.log('setting hashmap: '+'topstores:'+portalType+':'+dateCreated);
			client.hmset('topstores:'+portalType+':'+dateCreated, dataToWrite, function(err, topStoreWrite){
					if (err) {
						console.log('error writing topstores');
					}
					console.log('writeTopStores: '+'topstores:'+portalType+':'+dateCreated+'  result: '+JSON.stringify(topStoreWrite));
				});
		});
	}
}

connectToRedis();

module.exports = {
	checkTopStoreEntries: checkTopStoreEntries,
	createTopStoreEntries: createTopStoreEntries,
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
