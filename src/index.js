import async from 'async';
import { backoff, chunkItems } from './helpers';

class BetterBatch {
    constructor(docClient, maxRetries = 10, backoffFunction = backoff) {
        this.docClient = docClient;
        this.maxRetries = maxRetries;
        this.backoffFunction = backoffFunction;
    }
    
    writeChunk(chunk, callback) {
        let retryCount = 0;
        
        async.whilst(() => retryCount <= this.maxRetries, (callback) => {
            this.docClient.batchWrite(chunk, (err, data) => {
                if (err) return callback(err);
                
                if (Object.keys(data.UnprocessedItems).length) {
                    chunk.RequestItems = data.UnprocessedItems;
                    
                    retryCount += 1;
                    return setTimeout(() => callback(), this.backoffFunction(retryCount));
                }
                
                retryCount = 0;
                callback();
            });
        }, callback);
    }
    
    batchWrite(params, callback) {
        const chunks = chunkItems(params);
        
        async.eachSeries(chunks, (chunk, callback) => {
            this.writeChunk(chunk, callback);
        }, (err) => {
            if (err) return callback(err);
            
            callback();
        });
    }
}

export default BetterBatch;
