import Promise from 'bluebird';
import Redis from 'ioredis';
import uuid from 'node-uuid';
import eventEmitter from 'event-emitter';
import { all, and, contains, equals, isNil, map, not, toLower } from 'ramda';
import { NoOperationError } from './helpers/extendable-error';

require('babel-core/register');

export default function (redisURL = 'redis://localhost:6379') {
  // hourglass constants
  const hourglass = {};
  const redis = new Redis(redisURL);
  const sub = new Redis(redisURL);
  const emitter = eventEmitter({});
  const ttlHashSuffix = '-ttl-hash'; // time to live suffix
  const dataHashSuffix = '-data-hash';
  const suspendedSuffix = '-suspended';
  const resumedSuffix = '-resumed';
  const EVENT_DELETED = '__keyevent@0__:del';
  const EVENT_EXPIRED = '__keyevent@0__:expired';
  const EVENT_EXPIRE = '__keyevent@0__:expire';

  /*
  * Update function that targets redis keys to ensure expiration is triggered
  */
  function update() {

  }

  /**
  * Redis side function. Starts a timer with a set amount of milliseconds and data
  * @param {string} timerId - the id of the timer to start in redis
  * @param {number} timeInMS - the time in milliseconds for the timer
  * @param {object} data - the data to attach to a timer
  * @throws {NoOperationError} throws if a timer could not be started in redis
  * @return {Promise<string>} the string 'ok' if timer successfuly starts
  */
  function startTimer(timerId, timeInMS, data) {
    const pipeline = redis.pipeline();
    const stringifiedData = JSON.stringify(data);

    pipeline.hset(hourglass.timerHashId, timerId, timeInMS);
    pipeline.hset(hourglass.dataHashId, timerId, stringifiedData);
    pipeline.psetex(timerId, timeInMS, stringifiedData);

    return pipeline.exec()
    .then((results) => {
      map((result) => {
        if (!equals(toLower(result[1]), 'ok')) {
          throw new NoOperationError(`Could not start the timer with id: ${timerId}.`);
        }
      }, results);

      return 'ok';
    });
  }

  /**
  * Deletes a timer from the timer and data hashes and the expring key in redis
  * @param {string} timerId - the id of the timer to delete
  * @throws {NoOperationError} throws if a timer trying to be deleted doesn't exist
  * @return {Promise<object>} - returns an object containing the data of the timer
  */
  function deleteTimer(timerId) {
    const pipeline = redis.pipeline();
    const stringifiedData = redis.hget(hourglass.dataHashId, timerId);
    const data = JSON.parse(stringifiedData);

    pipeline.hdel(hourglass.timerHashId, timerId);
    pipeline.hdel(hourglass.dataHashId, timerId);
    pipeline.del(timerId);

    return pipeline.exec()
    .then((results) => {
      map((result) => {
        if (equals(result[1], 0)) {
          throw new NoOperationError('The key trying to be deleted doesn\'t exist.');
        }
      }, results);

      return data;
    });
  }

  function getHashKeys() {

  }

  function getTimeLeft() {

  }

  function suspendTimer() {

  }

  function resumeTimer() {

  }

  function getTimerData() {

  }

  /**
  * Private Redis based function.
  * Deletes from both redis hashes containing the hourglass timers and their data
  * @param {string} field - The field to delete from the redis hashes
  */
  function deleteFromHashes(field) {
    const pipeline = redis.pipeline();
    pipeline.hdel(hourglass.timerHashId, field);
    pipeline.hdel(hourglass.dataHashId, field);

    return pipeline.exec()
    .then((results) => {
      map((result) => {
        if (equals(result[1], 0)) {
          throw new NoOperationError('The key trying to be deleted doesn\'t exist.');
        }
      }, results);

      return true;
    });
  }

  // Initialize the library
  function init() {
    hourglass.globalHashId = uuid.v4();
    hourglass.timerHashId = hourglass.globalHashId + ttlHashSuffix;
    hourglass.dataHashId = hourglass.globalHashId + dataHashSuffix;

    // Redis Pub/Sub config settings
    redis.config('SET', 'notify-keyspace-events', 'KEA');

    // Event handler for Redis Pub/Sub events with the subscribing Redis client
    sub.on('message', (channel, message) => {
      if (equals(channel, EVENT_DELETED)) {
        if (contains(suspendedSuffix, message)) { emitter.emit('suspended', message); }
        else if (contains(dataHashSuffix, message)) { hourglass.dataHashId = null; }
        else if (contains(ttlHashSuffix, message)) { hourglass.timerHashId = null; }
        else { emitter.emit('deleted', message); }

        if (all(isNil)(hourglass.timerHashId, hourglass.dataHashId)) {
          hourglass.globalHashId = uuid.v4();
          hourglass.timerHashId = hourglass.globalHashId + ttlHashSuffix;
          hourglass.dataHashId = hourglass.globalHashId + dataHashSuffix;
        }
      }
      else if (and(equals(channel, EVENT_EXPIRED), not(contains(ttlHashSuffix, message)))) {
        deleteFromHashes(message);

        hourglass.getTimerData(message)
        .then(timerObj => emitter.emit('expired', timerObj));
      }
      else if (and(equals(channel, EVENT_EXPIRE), contains(resumedSuffix, message))) {
        emitter.emit('resumed', message);
      }
    });

    // Subscribe to the Redis Pub/Sub events with the subscribing Redis client
    sub.subscribe(EVENT_DELETED, EVENT_EXPIRED, EVENT_EXPIRE);

    // Setup the update function
    setInterval(update, 1000);
  }

  init();

  /**
  * Sets up event-emitter events to react to Redis Pub/Sub
  * Current supported internal events: deleted, expired, suspended, and resumed
  * @param {string} event - the supported event name to listen for
  * @param {function} callback - the callback function passed to event-emitter
  */
  hourglass.on = (event, callback) => emitter.on(event, callback);

  /**
  * Starts a timer in Redis
  * @param {string} timeInMS - The timer length in milliseconds
  * @param {Object} data - data object to be associated with the timer
  * @returns {Promise<String|Error>} - Resolves to the started timer id
  */
  hourglass.startTimer = (timeInMS, data = {}) => {
    const timerId = uuid.v4();

    return startTimer(timerId, timeInMS, data)
    .then(() => timerId);
  };

  hourglass.suspendTimer = () => {

  };

  hourglass.resumeTimer = () => {

  };

  hourglass.deleteTimer = timerId => deleteTimer(timerId);

  hourglass.getTimer = () => {

  };

  hourglass.getAllTimers = () => {

  };

  return hourglass;
}
