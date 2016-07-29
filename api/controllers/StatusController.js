/**
 * StatusController
 *
 * @description :: Server-side logic for managing statuses
 * @help        :: See http://sailsjs.org/#!/documentation/concepts/Controllers
 */

var mapStatusKeys = function(portalType, callback) {
	RedisConn.retrievePortalStatusKeys(portalType.toLowerCase(), function(err, statusKeys){
		if (err) callback(err,[]);
		if (statusKeys.length > 0) {
			var mappedStatuses = [];
			statusKeys.forEach(function(key, index, array){
				RedisConn.retrievePortalStatus(key, function(err,returnedStatus){
					if (err) callback(err,{});
					mappedStatuses.push(returnedStatus);
					if (index === array.length-1) {
						callback(err,mappedStatuses);
					}
				});
			});			
		} else {
			callback(err, []);
		}
	});
}

module.exports = {
	list: function(req,res) {
		if (['giftcard','online'].indexOf(req.param('portalType').toLowerCase()) !== -1) {
			mapStatusKeys(req.param('portalType'), function(err,returnedStatus){
				if (err) return res.view('status/list', {title: 'RewardYour$elf Status', status: []});
				RedisConn.retrievePortalNames(function(err, portalNames){
					if (err) return res.view('status/list', {title: 'RewardYour$elf Status', status: []});			
					return res.view('status/list',
						{
						 title: 'RewardYour$elf Status',
						 status: returnedStatus,
						 names: portalNames 
						});
				});
			});
		}
	}
};

