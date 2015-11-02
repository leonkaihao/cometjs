'use strict';
/**
 * Comet module.
 * @module comet
 * @author HaoKai
 */
var EventEmitter = require('events').EventEmitter;
var uuid = require('uuid');
var aes = require('../crypto/aes');

/** 
 * Create a comet
 * @constructor
 * @param {string} id - Comet id, if it is null then Comet will generate one randomly.
 * @param {Object} req - A http or express request object.
 * @param {Object} resp - A http or express response object.
 */
function Comet (id, req, resp) {
    this.CODE = {
        CLOSE_TIMEOUT: 'CLOSE_TIMEOUT',
        CLOSE_PEER:    'CLOSE_PEER',
        CLOSE_COMPEL:  'CLOSE_COMPEL',
    };
    Object.freeze(this.CODE); //make this.CODE read only
    if (!id) {
        id = uuid.v4();
    }
    this.id = id;
    this.req = req;    
    this.resp = resp;
    this.CONTENT_TYPE = 'multipart/x-mixed-replace;boundary=ctalk_boundary';
    this.SEND_CONTENT_TYPE_DEV = 'multipart/x-mixed-replace;boundary=ctalk_boundary';
    this.SEND_CONTENT_TYPE_APP = 'application/x-cloud-snipd';
    this.eventPool = {};
    this.loopTimeout = 5 * 60 * 1000; //init 5 minute
    this.timeId = 0;
    this.closeReason = null;
    this.closeFlag = false;

    /**    
    * Set close event callback
    * @param {Comet~closeCallback} cb - additional handling of the close event
    */    
    this.onClose = function (cb) {
        this.closeFunc = cb;
        return this;
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~closeCallback
     * @param {Object} event - The close event info.
     * @param {string} event.code - The event code, may be one of 'CLOSE_TIMEOUT', 'CLOSE_PEER' and 'CLOSE_COMPEL'.
     * @param {string} event.message - The event additional description.
     */

    /**    
    * Set error event callback    
    * @param {Comet~errorCallback} cb - additional handling of the error event
    */    
    this.onError = function (cb) {
        this.errFunc = cb;
        return this;
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~errorCallback
     * @param {Object} event - The error event info, is an Error object.
     * @param {string} event.message - The event additional description.
     */

    /**    
    * Set send event callback    
    * @param: {Comet~sendCallback} cb - additional handling of the send event
    */    
    this.onSend = function(cb) {
        this.sendFunc = cb;
        return this;
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~sendCallback
     * @param {Object} event - The send event info, include the detail of a send.
     * @param {string} event.cid - Comet id.
     * @param {string} event.sid - Send id.
     * @param {Object} event.msg - msg object which will be sent to peer.
     */

    /**    
    * Set echo event callback    
    * @param: {Comet~echoCallback} cb - additional handling of the echo event
    */    
    this.onEcho = function(cb) {
        this.echoFunc = cb;
        return this;
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~echoCallback
     * @param {Object} event - The echo event info, include the detail of a echo.
     * @param {string} event.cid - Comet id of the sender.
     * @param {string} event.sid - Send id.
     * @param {Object} event.msg - msg object which got from peer.
     */

    /**    
    * Set comet remove event callback    
    * @param: {Comet~rmCallback} cb - additional handling of the remove event
    */    
    this.onRemove = function(cb) {
        this.rmFunc = cb;
        return this;        
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~rmCallback
     * @param {Object} event - The remove event info.
     * @param {string} event.cid - Id of a Comet to be removed.
     */

    /**    
    * Set heart beat event callback    
    * @param: {Comet~hbCallback} cb - additional handling of the hearbeat event
    */    
    this.onHeart = function(cb) {
        this.hbFunc = cb;
        return this;        
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~hbCallback
     * @description this function has no param, only a trigger for additional actions 
     */

    /**    
    * Set data pack event callback    
    * @param: {Comet~pkCallback} cb - additional handling of the pack event
    */    
    this.onPack = function(cb) {
        this.pkFunc = cb;
        return this;
    };
    /**
     * This callback is displayed as part of the Comet class.
     * @callback Comet~pkCallback
     * @param {object} event - The pack event info.
     * @param {Comet~pkCallback~resultCallback} cb - callback function for receiving packed data asynchronously.
     * @description Event object include all data need to be sent, you can do additional processing like compressing or encoding.
     */
    /**
     * This callback is displayed as part of the comet pack callback function.
     * @callback Comet~pkCallback~resultCallback
     * @param {object} err - Value is null if no error, else is an Error object.
     * @param {string|Buffer} data - Not null if no error.
     */

    /**    
    * start comet running loop after all configuration    
    */    
    this.start = function() {
        var self = this;
        //config request
        self.req.connection.setTimeout(0); //never timeout
        //config response
        self.resp.writeHead(200, {'Content-Type': self.CONTENT_TYPE, 'Connection': 'close'});
        self._send({"cmd_echo": "connectOK"}, '--ctalk_boundary\r\n', this.SEND_CONTENT_TYPE_DEV);
        self.resp.on('close', function () {
            if (self.closeFlag) {
                console.warn('peer close duplicate, last close reason', self.closeReason);
                return;
            }
            self.closeFlag = true;
            self.closeReason = self.CODE.CLOSE_PEER;
            self.closeFunc({code: self.CODE.CLOSE_PEER, message: 'closed by peer'});
            self._remove();
        });
        self._setTimeOut(self.loopTimeout);

    };
    /**    
    * Send a message and wait for echo    
    */    
    this.sendMsg = function (msg, contentType, echoCb) {
        var sid = uuid.v4();
        msg.sid = sid;               //add transaction id for device echo
        //listening echo
        var echoEvent = new EventEmitter();
        this.eventPool[sid] = echoEvent;
        if (this.sendFunc) {
            this.sendFunc({cid: this.id, sid: sid, msg: msg});
        }
        if (!contentType) {
            contentType = this.SEND_CONTENT_TYPE_APP;
        }
        this._send(msg, '', contentType);

        var self = this;
        echoEvent.on("echo", function(echoMsg){
            echoCb(echoMsg);
            delete self.eventPool[echoMsg.sid];
            if (self.echoFunc) {
                self.echoFunc({cid: this.id, sid: sid, msg: echoMsg});
            }
        });
    };

    /**    
    * Send a message and do not wait for any echo
    */    
    this.directSend = function (msg) {
        this._send(msg);
    };

    /**    
    * Echo a message corresponding to a send    
    */    
    this.echoMsg = function (msg) {
        var sid = msg.sid;
        var echoEvent = this.eventPool[sid];
        if (echoEvent) {
            echoEvent.emit('echo', msg);
        } else {
            console.log('event for', sid, 'not found');
        }
    };

    /**    
    * Pong a message back after a client ping    
    */    
    this.pongMsg = function (msg, contentType) {
        var self = this;
        var interval = msg.interval;
        if(interval > 0){
            self.loopTimeout = interval * 2000;
        }
        self._send(msg, '', contentType);

        self._setTimeOut(self.loopTimeout); 
        if (self.hbFunc) {
            self.hbFunc();
        }
    };

    /**
     * release a comet object, used by cometMgr
     */
    this.del = function() {
        var self = this;
        if (self.closeFlag) {
            console.warn('Compel close duplicate, last close reason', self.closeReason);
            return;
        }
        self.closeFlag = true;

        self.closeReason = self.CODE.CLOSE_COMPEL;
        self._close();
        self._remove();
    };

    //internal   
    this._send = function (msg, prefix, contentType) {
        if (!prefix) {
            prefix = '';
        }
        if (!contentType) {
            contentType = this.SEND_CONTENT_TYPE_APP;
        }
        var msgStr = JSON.stringify(msg);
        var msgPk = msgStr;
        if (this.pkFunc) {
            this.pkFunc(msgPk, function(err, data) {
                if (err) {
                    this.errFunc(err);
                    return;
                }
                msgPk = data;

                var bufData = new Buffer(msgPk);
                //push to device
                var fmtHeader = prefix + 'Content-Type: ' + contentType + 
                '\r\nContent-Length: '+ bufData.length + '\r\n\r\n';
                var fmtTail = '\r\n--ctalk_boundary';
                var bufChunks = [Buffer(fmtHeader), bufData, Buffer(fmtTail)];
                var bufAll = Buffer.concat(bufChunks);

                console.log('comet push msg :',msgStr);
                this.resp.write(bufAll);
            });
        } else {
            var bufData = new Buffer(msgPk);
            //push to device
            var fmtHeader = prefix + 'Content-Type: ' + contentType + 
            '\r\nContent-Length: '+ bufData.length + '\r\n\r\n';
            var fmtTail = '\r\n--ctalk_boundary';
            var bufChunks = [Buffer(fmtHeader), bufData, Buffer(fmtTail)];
            var bufAll = Buffer.concat(bufChunks);

            console.log('comet push msg :',msgStr);
            this.resp.write(bufAll);
        } 
    };

    this._close = function(reason) {
        if (!reason) {
            reason = this.CODE.CLOSE_COMPEL
        }
        this.closeFunc({code: reason, message: 'closed by server'});
        this.resp.end();
    };

    //internal
    this._error = function (err) {
        if (this.errFunc) {
            this.errFunc(err);
        }
    };

    this._remove = function () {
        if (this.timeId != 0 ){
            clearTimeout(this.timeId);
            this.timeId = 0;
        }
        if (this.rmFunc) {
            this.rmFunc({cid: this.id});
        }
    };

    this._setTimeOut = function (msec) {
        var self = this;
        if (self.timeId != 0 ){
            clearTimeout(self.timeId);
            self.timeId = 0;
        }
        self.timeId = setTimeout(function() { 
            if (self.closeFlag) {
                console.warn('Timeout close duplicate, last close reason', self.closeReason);
                return;
            }
            self.closeFlag = true;
            self.closeReason = self.CODE.CLOSE_TIMEOUT;
            
            self.timeId = 0;            
            var errMsg = 'Get next ping timeout.';
            self._error(new Error(errMsg));
            self._close(self.CODE.CLOSE_TIMEOUT);
            self._remove();
        }, msec); 
    };
    return this;
}




/*
* comet manager
*/
var cometMgr = exports = module.exports = {    
    comets: {}, 
    sendIds: {},
    keyMap: {},
    /**    
    * API    
    */    
    createComet: function (cid, req, resp) {
        var self = this;
        if (cid && self.comets[cid]) {
            self.comets[cid].del();
        }
        var comet = new Comet(cid, req, resp);
        self.comets[comet.id] = comet; 
        comet.onSend(function(sinfo) {
            self._regSendId(sinfo.cid, sinfo.sid);
        }).onEcho(function(einfo) {
            self._unregSendId(einfo.sid);
        }).onRemove(function(rinfo) {
            self._deleteComet(rinfo.cid);
        }).onHeart(function(){
            if (this.key) {
                self.keyMap[this.key].alive = Date.now();
            }
        });
        return comet;    
    },    
    /**    
    * API    
    */    
    getComet: function (cid) {    
        return this.comets[cid];    
    },    

    mapComet: function (key, cid) {
        if (!this.keyMap[key]) {
            this.keyMap[key] = {visits: 0, lastVisit: Date.now(), alive: Date.now()};
        }
        var elem = this.keyMap[key];
        elem.cid = cid;
        elem.visits++;
        elem.lastVisit = Date.now();

        if (this.comets[cid]) {
            this.comets[cid].key = key;
        }
    },

    findComet: function (key) {
        var elem = this.keyMap[key];
        if (!elem) {
            console.error('Can not find comet from key', key);
            return null;
        }
        return this.comets[elem.cid];
    },

    getCometsCountLive: function() {
        var count = 0;
        for (var comet in this.comets) {
            count++;
        }
        return count;
    },

    getCometsCountVisit: function() {
        var count = 0;
        for (var key in this.keyMap) {
            count++;
        }
        return count;
    },

    getCometsExp: function () {
        var expComets = [];
        for (var cid in this.comets) {
            var exp = {cid: cid};
            var key = this.comets[cid].key;
            var comet = this.comets[cid];
            if (!key) {
                exp.info = 'comet object has no key record';
                expComets.push(exp);
            } else {
                exp.key = key;
                var elem = this.keyMap[key];
                if (!elem) {
                    exp.info = 'comet object has no key map';
                    expComets.push(exp);
                } else {
                    var idle = Date.now() - elem.alive;
                    if (idle > 60*1000) {
                        exp.idle = idle;
                        exp.info = 'comet object idle than ' + idle + 'ms';
                        expComets.push(exp);
                    }
                }
            }
        }
        return expComets;
    },  

    //internal
    _regSendId: function (cid, sid) {
        this.sendIds[sid] = cid;  
        console.log('comet:', cid, 'add sender', sid);
    },
    //internal
    _unregSendId: function (sid) {
        console.log('comet:', this.sendIds[sid], 'remove sender', sid);
        delete this.sendIds[sid];
    },
    /**    
    * API    
    */ 
    getCometBySendId: function (sid) {
        var cid = this.sendIds[sid];
        if (!cid) {
            return null;
        }
        return this.comets[cid]; 
    },
    //internal
    _deleteComet: function (cid) {
        delete this.comets[cid];
        for (var sid in this.sendIds) {
            if (this.sendIds[sid] == cid) {
                delete this.sendIds[sid];
            }
        }
    }
};
