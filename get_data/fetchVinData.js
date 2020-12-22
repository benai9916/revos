const fs = require('fs');
var elasticsearch = require('elasticsearch');
const outputFileName = 'AIMA_OFFICE.csv'
 
let data = {
    index: 'events-rev-es-vehicle000',
    type: '',
    size: '10000',
    scroll: '10m',
    body: {
      query: {
        "match": {
          "vin": "AIMA_OFFICE",
        }
      },
    }
  }
 
 
let client = new elasticsearch.Client({
  host: 'https://search-rev-elastic-pavbwqniud3xfj2ufsvvrkiive.ap-south-1.es.amazonaws.com',
});

 
fetchAll = (event, context, callback) => { 
  var json = [];
client.search(data , function getMoreUntilDone(error, response) {
  console.log(response)
  // collect all the records]
  response.hits.hits.forEach(function (hit) {
    json.push(hit);
  });
  console.log('data fetched: ', json.length);
 
  if (response.hits.total !== json.length) {
    // now we can call scroll over and over
    client.scroll({
      scrollId: response._scroll_id,
      scroll: '10m'
    }, getMoreUntilDone);
  } else {
        console.log('all done', json.length);
        let fields = Object.keys(json[json.length -1])
        // let objFieldsSource = Object.keys(json[json.length -1]['_source']);
        let objFieldsSource = [
        'vin',
        'tripId',
        'type',
        'timestamp',
        'batteryCurrent',
        'batteryVoltage', 
        'latitude', 
        'longitude',
        'throttle', 
        'wheelRpm', 
        'underVoltageLimit', 
        'overVoltageLimit', 
        'gps_speed'];

        let replacer = function(key, value) { return value === null ? '' : value }
        console.log(objFieldsSource)
        let csv = json.map(function(row){
        return fields.map(function(fieldName){
            if( fieldName === '_source'){
                return objFieldsSource.map(function(objFieldName){
                  
                    return JSON.stringify(row[fieldName][objFieldName], replacer)
                })
            }
            return JSON.stringify(row[fieldName], replacer)
        })
        })
 
        let index = fields.indexOf('_source');
          if (index !== -1) fields.splice(index, 1);
          fields.push(objFieldsSource);
          try {
          if (fs.existsSync('out.csv')) {
              console.log('file exist');
          }
          else {
            csv.unshift(fields) // add header column
          }
          } catch(err) {
              console.error(err)
          }
 
        fs.appendFileSync(outputFileName, csv.join('\n') + '\n')
  }
});
};
 
fetchAll()