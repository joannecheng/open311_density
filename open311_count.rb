require 'pg'
require 'json'

conn = PG.connect(:dbname => 'my_spatial_db')

query = "select (cook_census_data.p001001/st_area(cook_census_tracts.geom)) as density, service_code, cook_census_tracts_id, count(cook_census_tracts_id),
st_asgeojson(cook_census_tracts.geom) as geojson
from requests
left join cook_census_tracts on cook_census_tracts_id = cook_census_tracts.gid
left join cook_census_data on cook_census_data.geoid = cook_census_tracts.geoid10
group by cook_census_tracts_id, density, geojson, service_code
"

results = conn.exec(query)
features = { :type => 'FeatureCollection', :features => [] }
results.each do |row|
  feature = { 
    :type => 'Feature', 
    :geometry => JSON.parse(row['geojson']),
    :properties => { 
      :density => row['density'].to_f, 
      :tract_id => row['tract_id'],
      :request_count => row['count'].to_i, 
      :service_code => row['service_code'] }
  }
  features[:features] << feature 
end

puts features

conn.close

