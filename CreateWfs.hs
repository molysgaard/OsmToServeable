{-# LANGUAGE TypeSynonymInstances, MultiParamTypeClasses #-}

import Database.HDBC -- Database
import Database.HDBC.PostgreSQL -- Postgres

import Control.Monad (liftM)

import System.Environment (getArgs)

import qualified Data.Map as Map
import Data.List hiding (group)
import Data.String.Utils (split)

import Data.Convertible -- To write my own instance of convertible. This allows us to represent HStore collumns as Map String String.

import Data.ByteString.UTF8 (fromString)

-- not used anymore
--cFromConn = connectPostgreSQL "dbname=mapserver user=openstreetmap password=openstreetmap"
--cToConn = connectPostgreSQL "dbname=lys_wfs user=openstreetmap password=openstreetmap"

parseConn :: [String] -> Maybe (String, String, String)
parseConn args
  | length args == 3
  && "dbname=" `isPrefixOf` (args !! 0)
  && "user=" `isPrefixOf` (args !! 1)
  && "password=" `isPrefixOf` (args !! 2)
  = Just ((drop 7 (args !! 0)), (drop 5 (args !! 1)), (drop 9 (args !! 2)))
  | otherwise = Nothing

parseArgs :: [String] -> Maybe ((String, String, String),(String, String, String))
parseArgs args
  | length args == 8
  && (args !! 0) == "source"
  && (args !! 4) == "sink"
  = do 
     so <- parseConn (take 3 (drop 1 args))
     si <- parseConn (take 3 (drop 5 args))
     return (so, si)
  | otherwise = Nothing

createConnection (db,user,pass) = connectPostgreSQL ("dbname="++db++" user="++user++" password="++pass)

-- | This function takes the tags and relation tags of a object and based on that decide if it's an area or a closed way.
isArea [_,_,tags,rel_tags]
  | Just "yes" <- Map.lookup "area" ((fromSql tags) :: Hstore) = True
  | Just "yes" <- Map.lookup "airspace" ((fromSql tags) :: Hstore) = True
  | Just "aerodrome" <- Map.lookup "aeroway" ((fromSql tags) :: Hstore) = True
  | Just "apron" <- Map.lookup "aeroway" ((fromSql tags) :: Hstore) = True
  | Just "helipad" <- Map.lookup "aeroway" ((fromSql tags) :: Hstore) = True
  | Just "terminal" <- Map.lookup "aeroway" ((fromSql tags) :: Hstore) = True
  | otherwise = False

group p as = helper p as [] []
  where helper p (a:as) tr fa
          | p a = helper p as (a:tr) fa
          | otherwise = helper p as tr (a:fa)
        helper _ [] tr fa = (tr,fa)

eqConn :: Connection -> Connection -> IO Bool
eqConn a b = do
    da <- getTables a
    db <- getTables b
    if (length da /= length db)
      then return False
      else do
        bools <-  mapM (\(ta,tb) -> eqTable a ta b tb) $ zip da db
        return $ allTrue bools

allTrue [] = True
allTrue (False:_) = False
allTrue (True:bs) = allTrue bs

eqTable :: Connection -> String -> Connection -> String -> IO Bool
eqTable a ta b tb = do
  dta <- describeTable a ta
  dtb <- describeTable b tb
  return $ dta == dtb

-- | When there are no tags the query returns []
-- When there are tags, the query returns [["blablabla"]]
-- We need to detect this
extractTags [] = SqlNull
extractTags a = head . head $ a

main :: IO ()
main = do
  args <- getArgs
  case parseArgs args of
    Nothing -> do
      putStrLn $ "Invalid arguments. syntax is: CreateWfs source dbname=sourcename user=sourceuser password=sourcepass sink dbname=sinkname user=sinkuser password=sinkpassword\n" ++ show args
    Just (so, si) -> do
      fc <- createConnection so
      tc <- createConnection si
      createDB fc tc

createDB fc tc = do
    createNodes fc tc
    createWaysAndAreas fc tc

-- | master function for creating ways.
-- Takes the from and to connection.
-- First it takes all the id's of the ways
-- Then uses 'createWay' to generate all the ways.
-- Inserts them into the toConn database
createNodes fromConn toConn = do
    ret <- eqConn fromConn toConn
    if ret
      then error "To and from connection cant be the same."
      else do
        ids <- liftM (map head) $ quickQuery' fromConn "SELECT id from nodes WHERE (tags \\?| ARRAY['name','aerodrome','airspace','obstacle','navaid','vfrreportingpoint'])" [] :: IO [SqlValue]
        nodes <- mapM (createNode fromConn) ids
        run toConn "DROP TABLE nodes" []
        run toConn "CREATE TABLE nodes (id INTEGER NOT NULL, geom geometry, tags hstore, rel_tags hstore)" []
        mapM (run toConn "INSERT INTO nodes (id,geom,tags,rel_tags) VALUES (?,?,?,?)") nodes
        commit toConn
        
-- | Generates one way from a id
-- Gathers all the nodes that belong to that way in a linestring
-- Fetches the tags etc for the way
-- Finds all relations, prefixes each one and unions them together to one hstore
-- Returns a row with all the data
createNode :: Connection -> SqlValue -> IO [SqlValue]
createNode conn id = do
    node <- liftM head $ liftM (map head) $ quickQuery' conn "SELECT geom FROM nodes WHERE id=?" [id]
    tags <- liftM extractTags $ quickQuery' conn "SELECT tags FROM nodes WHERE id=?" [id]
    relationMemberships <- quickQuery' conn "SELECT relation_id FROM relation_members WHERE (member_type='N' AND member_id=?)" [id]
    relationTags <- liftM (map head) $ liftM (map head) $ mapM (\relId -> quickQuery' conn "SELECT tags FROM relations where id=?" [head relId]) relationMemberships
    let relTags = toSql $ Map.unions (map addPrefix ((map convert relationTags) :: [Hstore]))
    return [id,node,tags,relTags]

-- | master function for creating ways.
-- Takes the from and to connection.
-- First it takes all the id's of the ways
-- Then uses 'createWay' to generate all the ways.
-- Inserts them into the toConn database
createWaysAndAreas fromConn toConn = do
    ret <- eqConn fromConn toConn
    if ret
      then error "To and from connection cant be the same."
      else do
        ids <- liftM (map head) $ quickQuery' fromConn "SELECT id from ways" [] :: IO [SqlValue]
        nodeList <- liftM (map (map head)) $ mapM (\x -> quickQuery' fromConn "SELECT node_id FROM way_nodes WHERE way_id=? ORDER BY sequence_id" [x]) ids
        let (wayIds,areaIds) = partitionWayAreas (zip nodeList ids) isClosed

        polygons <- mapM (createArea fromConn) areaIds
        let (areas, closedWays) = group isArea polygons
        run toConn "DROP TABLE areas" []
        run toConn "CREATE TABLE areas (id INTEGER NOT NULL, geom geometry, tags hstore, rel_tags hstore)" []
        mapM (run toConn "INSERT INTO areas (id,geom,tags,rel_tags) VALUES (?,?,?,?)") areas

        openWays <- mapM (createWay fromConn) wayIds
        run toConn "DROP TABLE ways" []
        run toConn "CREATE TABLE ways (id INTEGER NOT NULL, geom geometry, tags hstore, rel_tags hstore)" []
        mapM (run toConn "INSERT INTO ways (id,geom,tags,rel_tags) VALUES (?,?,?,?)") (closedWays ++ openWays)

        commit toConn

-- | Generates one way from a id
-- Gathers all the nodes that belong to that way in a linestring
-- Fetches the tags etc for the way
-- Finds all relations, prefixes each one and unions them together to one hstore
-- Returns a row with all the data
createWay :: Connection -> SqlValue -> IO [SqlValue]
createWay conn id = do
    nodes <- liftM head $ liftM (map head) $ quickQuery' conn "SELECT ST_MakeLine(geom) FROM (SELECT geom FROM nodes JOIN (SELECT node_id,sequence_id FROM way_nodes WHERE way_id=?) AS ids ON nodes.id=ids.node_id ORDER BY ids.sequence_id) AS points" [id]
    tags <- liftM head $ liftM (map head) $ quickQuery' conn "SELECT tags FROM ways WHERE id=?" [id]
    relationMemberships <- quickQuery' conn "SELECT relation_id FROM relation_members WHERE (member_type='W' AND member_id=?)" [id]
    relationTags <- liftM (map head) $ liftM (map head) $ mapM (\relId -> quickQuery' conn "SELECT tags FROM relations where id=?" [head relId]) relationMemberships
    let relTags = toSql $ Map.unions (map addPrefix ((map convert relationTags) :: [Hstore]))
    return [id,nodes,tags,relTags]

createArea :: Connection -> SqlValue -> IO [SqlValue]
createArea conn id = do
    nodes <- liftM head $ liftM (map head) $ quickQuery' conn "SELECT ST_MakePolygon(ST_MakeLine(geom)) FROM (SELECT geom FROM nodes JOIN (SELECT node_id,sequence_id FROM way_nodes WHERE way_id=?) AS ids ON nodes.id=ids.node_id ORDER BY ids.sequence_id) AS points" [id]
    tags <- liftM head $ liftM (map head) $ quickQuery' conn "SELECT tags FROM ways WHERE id=?" [id]
    relationMemberships <- quickQuery' conn "SELECT relation_id FROM relation_members WHERE (member_type='W' AND member_id=?)" [id]
    relationTags <- liftM (map head) $ liftM (map head) $ mapM (\relId -> quickQuery' conn "SELECT tags FROM relations where id=?" [head relId]) relationMemberships
    let relTags = toSql $ Map.unions (map addPrefix ((map convert relationTags) :: [Hstore]))
    return [id,nodes,tags,relTags]

isClosed :: (Eq a) => [a] -> Bool
isClosed a = ((head a) == (last a)) && (length a > 2)

partitionWayAreas :: [(a,b)] -> (a -> Bool) -> ([b],[b])
partitionWayAreas as p = let helper [] _ w a = (w,a)
                             helper ((nodes,wayId):as) p ways areas
                              | p nodes = helper as p ways (wayId:areas)
                              | otherwise = helper as p (wayId:ways) areas
                         in helper as p [] []

-- | Prefixes all the keys of a map with the value of the key "name"
addPrefix m
  | Nothing <- Map.lookup "name" m
  = m
  | Just p <- Map.lookup "name" m
  = Map.mapKeys ((p++":") ++ ) m

-- | little utility
myDiv sep str = let [a,b] = split sep str
                in (a,b)

-- | Haskell representation of a hstore cell in postgres
type Hstore = Map.Map String String

-- | instances for converting between postgres and haskell representation of hstore
instance Convertible Hstore SqlValue where
    safeConvert m
      | Map.null m = return SqlNull
      | otherwise = return . toSql . fromString . (\x -> "\"" ++ x ++ "\"") . (intercalate sep) . (map test) $ Map.toList m
          where is = "\"=>\""
                sep = "\", \""
                test (k,v) = intercalate is [k,v]

instance Convertible SqlValue Hstore where
    safeConvert SqlNull = Right Map.empty
    safeConvert bs@(SqlByteString _) = let kvs = (split sep) . (\x -> init (tail x)) $ (fromSql bs)
                                       in Right . Map.fromList $ map (myDiv is) kvs
      where is = "\"=>\""
            sep = "\", \""
    safeConvert e = convError "Not hstore type" e
