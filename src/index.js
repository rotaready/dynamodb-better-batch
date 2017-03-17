import async from 'async';
import { backoff, chunkItems } from './helpers';

class BetterBatch {
    constructor(docClient, maxRetries = 10, backoffFunction = backoff) {
        this.docClient = docClient;
        this.maxRetries = maxRetries;
        this.backoffFunction = backoffFunction;
    }
    
    processChunk(chunk, operation, unprocessedCheck, callback) {
        let retryCount = 0;
        let shouldRetry = false;
        
        async.whilst(() => shouldRetry, (callback) => {
            operation(chunk, (err, data) => {
                if (err) return callback(err);
                
                const unprocessed = unprocessedCheck(data);
                
                if (unprocessed) {
                    chunk.RequestItems = unprocessed;
                    
                    if (retryCount < this.maxRetries) {
                        shouldRetry = true;
                        retryCount += 1;
                    } else {
                        shouldRetry = false;
                        return callback();
                    }
                    
                    return setTimeout(() => callback(), this.backoffFunction(retryCount));
                }
                
                shouldRetry = false;
                callback();
            });
        }, callback);
    }
    
    batchOperation(operation, chunks, unprocessedCheck, callback) {
        async.eachSeries(chunks, (chunk, callback) => {
            this.processChunk(chunk, operation, unprocessedCheck, callback);
        }, callback);
    }
    
    batchGet(params, callback) {
        const chunks = chunkItems(params, 100, true);
        
        this.batchOperation(this.docClient.batchGet, chunks, (data) => {
            if (Object.keys(data.UnprocessedKeys).length) {
                return data.UnprocessedKeys;
            }
        }, callback);
    }
    
    batchWrite(params, callback) {
        const chunks = chunkItems(params, 25);
        
        this.batchOperation(this.docClient.batchWrite, chunks, (data) => {
            if (Object.keys(data.UnprocessedItems).length) {
                return data.UnprocessedItems;
            }
        }, callback);
    }
}

export default BetterBatch;
