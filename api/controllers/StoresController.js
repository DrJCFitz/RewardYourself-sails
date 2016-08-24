/**
 * StoresController
 *
 * @description :: Server-side logic for managing controllers
 * @help        :: See http://sailsjs.org/#!/documentation/concepts/Controllers
 */

var mapStoreKeys = function(portalType, callback) {
	RedisConn.retrieveStoreCounts(portalType.toLowerCase(), function(err, storeCounts){
		if (err) callback(err,[]);
		if (storeCounts !== null 
			&& typeof storeCounts === 'object' 
			&& Object.keys(storeCounts).length > 0) {
	
			RedisConn.retrieveStoreNames(function(err,returnedNames){
				var mappedStores = [];
				if (err) callback(err,{});
				if (typeof returnedNames === 'object') {
					for (var key in storeCounts) {
						if (returnedNames[key] !== undefined && 
							returnedNames[key] !== null && 
							returnedNames[key] !== '') {

							mappedStores.push({key: key, name: returnedNames[key], count: parseInt(storeCounts[key])});
						}
					}					
				}
				callback(err,mappedStores);
			});
		} else {
			callback(err, []);
		}
	});
}


module.exports = {
	top: function(req,res) {
		if (['giftcard','online'].indexOf(req.param('portalType').toLowerCase()) !== -1) {
			// this cron is scheduled to update daily at 13:10 UTC. Return the previous day's data if before 13:30 UTC
			var today = new Date();
			var nowToUTC = today.getTime() - today.getTimezoneOffset() * 60 * 1000; 
			var dayUTCLong = Date.UTC(today.getUTCFullYear(),
				today.getUTCMonth(),
				((nowToUTC < Date.UTC(today.getUTCFullYear(), 
					today.getUTCMonth(), 
					today.getUTCDate(), 
					13,30) ? 
					today.getUTCDate()-1 : 
					today.getUTCDate()))) / 1000;

			DynamoDBConn.queryTopStores(req.param('portalType').toLowerCase(), dayUTCLong, function(err,returnedStores){
				if (returnedStores !== null && returnedStores.Items !== undefined) {
					return res.json(returnedStores.Items);					
				} else {
					console.log('check connection to database');
					return res.json([]);
				}
			});
		} else {
			return res.json([]);
		}
	},
	list: function(req,res) {
		if (['giftcard','online'].indexOf(req.param('portalType').toLowerCase()) !== -1) {
			mapStoreKeys(req.param('portalType'), function(err,returnedStatus){
				return res.json(returnedStatus);
			});
		} else {
			return res.json([]);
		}
	},
	detail: function(req,res) {
		if (['giftcard','online'].indexOf(req.param('portalType').toLowerCase()) !== -1) {
			DynamoDBConn.queryStoreRewards(req.param('storeKey').toLowerCase(), function(err,storesResult){
				if (storesResult !== null && storesResult.Items !== undefined) {
					return res.json(storesResult.Items);
				} else {
					console.log('check connection to database');
					return res.json([]);
				}
			});
		} else {
			return res.json([]);
		}
	}
};

