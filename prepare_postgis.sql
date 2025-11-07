-- Prepare buildings table
ALTER TABLE buildings
ADD COLUMN geom_4326 geometry,
ADD COLUMN geom_3857 geometry;

UPDATE buildings
SET geom_4326 = ST_Transform(ST_MakeValid(wkb_geometry), 4326),
    geom_3857 = ST_Transform(ST_MakeValid(wkb_geometry), 3857);

CREATE INDEX geom_3857_idx_buildings ON buildings USING GIST(geom_3857);
CREATE INDEX geom_4326_idx_buildings ON buildings USING GIST(geom_4326);

-- Prepare hydrants table
ALTER TABLE hydrants
ADD COLUMN geom_4326 geometry,
ADD COLUMN geom_3857 geometry;

UPDATE hydrants
SET geom_4326 = ST_Transform(ST_MakeValid(wkb_geometry), 4326),
    geom_3857 = ST_Transform(ST_MakeValid(wkb_geometry), 3857);

CREATE INDEX geom_3857_idx_hydrants ON hydrants USING GIST(geom_3857);
CREATE INDEX geom_4326_idx_hydrants ON hydrants USING GIST(geom_4326);

-- Prepare neighborhoods table
ALTER TABLE neighborhoods
ADD COLUMN geom_4326 geometry,
ADD COLUMN geom_3857 geometry;

UPDATE neighborhoods
SET geom_4326 = ST_Transform(ST_MakeValid(wkb_geometry), 4326),
    geom_3857 = ST_Transform(ST_MakeValid(wkb_geometry), 3857);

CREATE INDEX geom_3857_idx_neighborhoods ON neighborhoods USING GIST(geom_3857);
CREATE INDEX geom_4326_idx_neighborhoods ON neighborhoods USING GIST(geom_4326);

-- Prepare census_blocks table
ALTER TABLE census_blocks
ADD COLUMN geom_4326 geometry,
ADD COLUMN geom_3857 geometry;

UPDATE census_blocks
SET geom_4326 = ST_Transform(ST_MakeValid(wkb_geometry), 4326),
    geom_3857 = ST_Transform(ST_MakeValid(wkb_geometry), 3857);

CREATE INDEX geom_3857_idx_census_blocks ON census_blocks USING GIST(geom_3857);
CREATE INDEX geom_4326_idx_census_blocks ON census_blocks USING GIST(geom_4326);
