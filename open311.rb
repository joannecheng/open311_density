require 'json'
require 'pg'
require 'date'
require 'typhoeus'
require 'active_support/all'

def get_requests(service_codes, url)
  requests = "requests.json?start_date=#{URI.encode (Time.now-12.hours).to_s}&end_time=#{URI.encode Time.now.to_s}&page_size=500"
  requests_by_code = {}
  service_codes.each do |sr|
    requests_by_code[sr] = JSON.parse(Typhoeus.get(url+requests+"&service_code=#{sr}").response_body)
  end
  requests_by_code
end

def update_db(requests_by_code)
  conn = PG.connect( :dbname => 'my_spatial_db')
  requests_table = 'requests'

  requests_by_code.keys.each do |sr|
    requests_by_code[sr].each do |request|
      query = "INSERT into #{requests_table} 
    (service_code, location, requested_datetime) 
    VALUES ( 
    '#{request['service_code']}', 
    ST_GeomFromText('POINT(#{request['long']} #{request['lat']})', 4326),
    '#{Date.parse(request['requested_datetime']) }' ) ;"
    conn.exec(query)
    end
  end

end

url = "http://311api.cityofchicago.org/open311/v2/"
services = "services.json"

all_services = JSON.parse Typhoeus.get(url+services).response_body
service_codes = all_services.map { |service| service['service_code'] }.compact

requests_by_code = get_requests(service_codes, url)
update_db(requests_by_code)
