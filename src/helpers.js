// See https://en.wikipedia.org/wiki/Exponential_backoff
// One fail = 0.5 seconds
// Two = 1.5 seconds
// Three = 3.5 seconds
function backoff(retryCount) {
    // eslint-disable-next-line no-restricted-properties
    return ((Math.pow(2, retryCount) - 1) / 2) * 1000;
}

function chunkItems(params, chunkSize, isGet) {
    // This will hold all of the table names
    const tableKeys = Object.keys(params.RequestItems);
    // We eventually return this
    const chunks = [];

    // Holds the size of the current chunk we're building
    let currentChunkSize = 0;
    // Gets pushed in to the array then reassigned
    let currentChunk = { RequestItems: {} };
    
    // Loop through each of the tables
    for (let k = 0; k < tableKeys.length; k += 1) {
        // The name of this table
        const tableKey = tableKeys[k];
        
        // This contains the children of the table (Keys et al if a get, just an array if a write)
        const requestItem = params.RequestItems[tableKey];
        
        // This holds the actual data we want to chunk
        let rowDataArray;
        
        if (isGet) {
            rowDataArray = requestItem.Keys;
        } else {
            rowDataArray = requestItem;
        }
        
        // The size of the last slice we built
        let lastSliceSize = 0;
        
        // Loop through the rowDataArray, adding lastSliceSize each time
        // This means we jump to the next bit of data that isn't in a chunk
        for (let i = 0; i < rowDataArray.length; i += lastSliceSize) {
            // Slice it, making sure we don't exceed the chunk size
            const rowDataSlice = rowDataArray.slice(i, i + (chunkSize - currentChunkSize));
            
            lastSliceSize = rowDataSlice.length;
            
            // Nothing will get overwritten by doing this
            // If the size of rowDataArray is > chunkSize, we move on to a new chunk below
            if (isGet) {
                const tableParams = Object.assign({}, requestItem);
                tableParams.Keys = rowDataSlice;
                
                currentChunk.RequestItems[tableKey] = tableParams;
            } else {
                currentChunk.RequestItems[tableKey] = rowDataSlice;
            }
            
            currentChunkSize += lastSliceSize;
            
            // If we've reached the chunk size, add the chunk and move on
            if (currentChunkSize === chunkSize) {
                chunks.push(currentChunk);
                currentChunk = { RequestItems: {} };
                currentChunkSize = 0;
            }
        }
    }
    
    // If the last chunk doesn't reach chunkSize, we just add it in
    if (currentChunkSize > 0) {
        chunks.push(currentChunk);
    }
    
    return chunks;
}

export { backoff, chunkItems };
