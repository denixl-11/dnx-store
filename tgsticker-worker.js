importScripts('rlottie-wasm.js');
importScripts('pako-inflate.min.js');

function RLottieItem(reqId, jsString, width, height, fps) {
  this.stringOnWasmHeap = null;
  this.handle = null;
  this.frameCount = 0;

  this.reqId = reqId;
  this.width = width;
  this.height = height;
  this.fps = Math.max(1, Math.min(60, fps || 60));

  this.dead = false;

  this.init(jsString, width, height);

  reply('loaded', this.reqId, this.frameCount, this.fps);
}

RLottieItem.prototype.init = function(jsString) {
  try {
    this.handle = RLottieWorker.Api.init();

    this.stringOnWasmHeap = allocate(intArrayFromString(jsString), 'i8', 0);

    this.frameCount = RLottieWorker.Api.loadFromData(this.handle, this.stringOnWasmHeap);

    RLottieWorker.Api.resize(this.handle, this.width, this.height);
  } catch(e) {
    console.error('init RLottieItem error:', e);
  }
};

RLottieItem.prototype.render = function(frameNo, clamped) {
  if (this.dead) return;

  var realFrameNo = frameNo;
  if (frameNo < 0) {
    realFrameNo = 0;
  } else if (frameNo >= this.frameCount) {
    realFrameNo = this.frameCount - 1;
  }

  try {
    RLottieWorker.Api.render(this.handle, realFrameNo);

    var bufferPointer = RLottieWorker.Api.buffer(this.handle);

    var data = Module.HEAPU8.subarray(bufferPointer, bufferPointer + (this.width * this.height * 4));

    if(!clamped) {
      clamped = new Uint8ClampedArray(data);
    } else {
      clamped.set(data);
    }

    reply('frame', this.reqId, frameNo, clamped);
  } catch(e) {
    console.error('Render error:', e);
    this.dead = true;
    reply('error', this.reqId, 'render_failed_' + frameNo + ':' + String(e && e.message || e));
  }
};

RLottieItem.prototype.destroy = function() {
  this.dead = true;
  if (this.handle) {
    RLottieWorker.Api.destroy(this.handle);
    this.handle = null;
  }
  // Every loaded TGS is copied onto the WASM heap.  The legacy player freed
  // only the renderer handle, so catalogue/case poster prewarming exhausted
  // the fixed heap and the next modal failed on frame 1.
  if (this.stringOnWasmHeap) {
    Module._free(this.stringOnWasmHeap);
    this.stringOnWasmHeap = null;
  }
};

var RLottieWorker = (function() {
  var worker = {};
  worker.Api = {};

  function initApi() {
    worker.Api = {
      // lottie_init returns the renderer handle. Declaring it as void made
      // every player lose that pointer, so destroy() could not release the
      // previous animation and the next catalogue item aborted on frame 0.
      init: Module.cwrap('lottie_init', 'number', []),
      destroy: Module.cwrap('lottie_destroy', '', ['number']),
      resize: Module.cwrap('lottie_resize', '', ['number', 'number', 'number']),
      buffer: Module.cwrap('lottie_buffer', 'number', ['number']),
      frameCount: Module.cwrap('lottie_frame_count', 'number', ['number']),
      render: Module.cwrap('lottie_render', '', ['number', 'number']),
      loadFromData: Module.cwrap('lottie_load_from_data', 'number', ['number', 'number']),
    };
  }

  worker.init = function() {
    initApi();
    reply('ready');
  };

  return worker;
}());

Module.onRuntimeInitialized = function() {
  RLottieWorker.init();
};

var items = {};
var cancelledItems = {};
var queryableFunctions = {
  loadFromData: function(reqId, url, width, height) {
    getUrlContent(url, function(err, data) {
      if (cancelledItems[reqId]) {
        delete cancelledItems[reqId];
        return;
      }
      if (err) {
        console.warn('Can\'t fetch file ' + url, err);
        reply('error', reqId, 'fetch_failed');
        return;
      }
      try {
        var json = pako.inflate(data, {to: 'string'});
        var json_parsed = JSON.parse(json);
        items[reqId] = new RLottieItem(reqId, json, width, height, json_parsed.fr);
      } catch (e) {
        console.warn('Invalid file ' + url);
        reply('error', reqId, 'invalid_tgs');
        return;
      }
    });
  },
  destroy: function(reqId) {
    if (items[reqId]) {
      items[reqId].destroy();
      delete items[reqId];
    } else {
      cancelledItems[reqId] = true;
    }
  },
  renderFrame: function(reqId, frameNo, clamped) {
    if (!items[reqId]) {
      reply('error', reqId, 'player_not_ready');
      return;
    }
    items[reqId].render(frameNo, clamped);
  }
};

function defaultReply(message) {
  // your default PUBLIC function executed only when main page calls the queryableWorker.postMessage() method directly
  // do something
}

/**
 * Returns true when run in WebKit derived browsers.
 * This is used as a workaround for a memory leak in Safari caused by using Transferable objects to
 * transfer data between WebWorkers and the main thread.
 * https://github.com/mapbox/mapbox-gl-js/issues/8771
 *
 * This should be removed once the underlying Safari issue is fixed.
 *
 * @private
 * @param scope {WindowOrWorkerGlobalScope} Since this function is used both on the main thread and WebWorker context,
 *      let the calling scope pass in the global scope object.
 * @returns {boolean}
 */
var _isSafari = null;
function isSafari(scope) {
  if(_isSafari == null) {
    var userAgent = scope.navigator ? scope.navigator.userAgent : null;
    _isSafari = !!scope.safari ||
    !!(userAgent && (/\b(iPad|iPhone|iPod)\b/.test(userAgent) || (!!userAgent.match('Safari') && !userAgent.match('Chrome'))));
  }
  return _isSafari;
}

function reply() {
  if(arguments.length < 1) { 
    throw new TypeError('reply - not enough arguments'); 
  }

  var args = Array.prototype.slice.call(arguments, 1);
  if(isSafari(self)) {
    postMessage({ 'queryMethodListener': arguments[0], 'queryMethodArguments': args });
  } else {
    var transfer = [];
    for(var i = 0; i < args.length; i++) {
      if(args[i] instanceof ArrayBuffer) {
        transfer.push(args[i]);
      }
  
      if(args[i].buffer && args[i].buffer instanceof ArrayBuffer) {
        transfer.push(args[i].buffer);
      }
    }

    postMessage({ 'queryMethodListener': arguments[0], 'queryMethodArguments': args }, transfer);
  }
}

onmessage = function(oEvent) {
  if(oEvent.data instanceof Object && oEvent.data.hasOwnProperty('queryMethod') && oEvent.data.hasOwnProperty('queryMethodArguments')) {
    queryableFunctions[oEvent.data.queryMethod].apply(self, oEvent.data.queryMethodArguments);
  } else {
    defaultReply(oEvent.data);
  }
};



function getUrlContent(path, callback) {
  try {
    var xhr = new XMLHttpRequest();
    var completed = false;
    var finish = function(err, data) {
      if (completed) return;
      completed = true;
      callback(err, data);
    };
    xhr.open('GET', path, true);
    if ('responseType' in xhr) {
      xhr.responseType = 'arraybuffer';
    }
    if (xhr.overrideMimeType) {
      xhr.overrideMimeType('text/plain; charset=x-user-defined');
    }
    xhr.onreadystatechange = function (event) {
      if (xhr.readyState === 4) {
        if (xhr.status === 200 || xhr.status === 0) {
          finish(null, xhr.response || xhr.responseText);
        } else {
          finish(new Error('Ajax error: ' + this.status + ' ' + this.statusText));
        }
      }
    };
    xhr.onerror = function () { finish(new Error('Network error')); };
    xhr.ontimeout = function () { finish(new Error('Network timeout')); };
    xhr.timeout = 8000;
    xhr.send();
  } catch (e) {
    callback(new Error(e));
  }
};
