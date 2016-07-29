/**
 * Status.js
 *
 * @description :: TODO: You might write a short summary of how this model works and what it represents here.
 * @docs        :: http://sailsjs.org/documentation/concepts/models-and-orm/models
 */

module.exports = {
  identity: 'status:choice:online',
  attributes: {

  	jquery: {
  		type: 'boolean'
  	},

  	rootElementExists: {
  		type: 'boolean'
  	},

  	elementCount: {
  		type: 'integer'
  	},

  	nameElementExists: {
  		type: 'boolean'
  	},

  	linkElementExists: {
  		type: 'boolean'
  	},

  	nameElementPopulated: {
  		type: 'boolean'
  	},

  	linkElementPopulated: {
  		type: 'boolean'
  	},

  	rewardElementPopulated: {
  		type: 'boolean'
  	},

  	portalKey: {
  		type: 'string'
  	},

  	portalType: {
  		type: 'string'
  	},

  	dateCreated: {
  		type: 'integer'
  	}

  }
};

