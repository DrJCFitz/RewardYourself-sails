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

var listOrUpdateTopStores = function(portalType, dateCreated, callback) {
	RedisConn.checkTopStoreEntries(portalType.toLowerCase(), dateCreated, function(err, stores){
		if (err) {
			callback(err);
		}
		//console.log('listOrUpdateTopStores stores: '+JSON.stringify(stores));
		if (null !== stores && undefined !== stores) {
			callback(null, stores);
		} else {
			//console.log('listOrUpdateTopStores query DynamoDB');
			DynamoDBConn.queryTopStores(portalType, dateCreated, function(err,returnedStores){
				if (err) {
					callback(err);
				}
				//console.log('listOrUpdateTopStores DynamoDB returned stores: '+JSON.stringify(returnedStores));
				if (returnedStores !== null && returnedStores.Items !== undefined) {
					var storeKeys = [];
					var filteredStores = {};
					returnedStores.Items.forEach(function(element, index, array){
						// unique entries only
						var storeKey = element['portalStoreKey'].substring(element['portalStoreKey'].indexOf(':')+1,element['portalStoreKey'].length);
						//console.log('listOrUpdateTopStores filtering duplicates: '+element.name+' is unique? '+JSON.stringify([storeKey, storeKeys.indexOf(storeKey)]));
						if (storeKeys.indexOf(storeKey) === -1) {
							storeKeys.push(storeKey);
							filteredStores[element.name] = element.topStoreRating;
						}
						if (index === (array.length-1)) {
							//console.log('listOrUpdateTopStores createTopStoreEntries: topstores'+portalType.toLowerCase()+dateCreated+' : '+JSON.stringify(filteredStores));
							RedisConn.createTopStoreEntries(portalType.toLowerCase(), dateCreated, filteredStores);
							callback(null, filteredStores);
						}
					});
				} else {
					callback('check connection to database');
				}
			});			
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

			listOrUpdateTopStores(req.param('portalType').toLowerCase(), dayUTCLong, function(err, returnedStores){
				if (returnedStores !== null && returnedStores !== undefined) {
					return res.json(returnedStores);					
				} else {
					console.log('something went wrong finding top stores for '+dayUTCLong);
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

