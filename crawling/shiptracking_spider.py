import scrapy

class ShipTracking(scrapy.Spider):
  name = "shiptracking_spider"
  start_urls = ['https://www.myshiptracking.com/ports-arrivals-departures/?mmsi=&pid=386&type=0&time=1553040000_1553126399&pp=50&page=%s' % page for page in range(1,64)]

  def parse(self, response):
    SET_SELECTOR = '.table-row'
    for row in response.css(SET_SELECTOR):
      EVENT_SELECTOR = '.col ::text'
      DATETIME_SELECTOR = '.col b ::text'
      PORT_SELECTOR = '.col a ::text'
      VESSEL_SELECTOR = '.col span a ::text'
      yield {
        'Event': row.css(EVENT_SELECTOR).extract_first(),
        'Datetime': row.css(EVENT_SELECTOR).extract()[1] + row.css(DATETIME_SELECTOR).extract_first(),
        'Port': row.css(PORT_SELECTOR).extract_first(),
        'Vessel': row.css(VESSEL_SELECTOR).extract_first()
      }