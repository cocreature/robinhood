module Main (main) where

import           Debug.Trace

import           Criterion
import           Criterion.IO
import           Criterion.Types
import           Data.Foldable
import           Data.List
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.List.Split
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Graphics.Rendering.Chart.Backend.Cairo
import           Graphics.Rendering.Chart.Easy
import           Statistics.Types
import           System.Environment
import           System.IO
import           Text.Read

main :: IO ()
main = do
  [file] <- getArgs
  res <- readJSONReports file
  case res of
    Left err -> putStrLn err
    Right (_, _, reports) ->
      case traverse process reports of
        Nothing -> putStrLn "Failed to process result"
        Just rs -> do
          let plotData = aggregate rs
          for_ plotData $ \p -> do
            let fileName = plotName p <> ".svg"
            putStrLn ("Generating " <> fileName)
            toFile (def & fo_format .~ SVG) fileName (generatePlot p)

-- We assume that

data Result = Result
  { operation :: String
  , implementation :: String
  , size :: Double
  , iters :: Estimate ConfInt Double
  } deriving Show

process :: Report -> Maybe Result
process r = Result op impl <$> readMaybe sizeStr <*> getIters r
  where
    (sizeStr:impl:rest) = reverse (splitOn "/" (reportName r))
    op = intercalate "/" (reverse rest)

getIters :: Report -> Maybe (Estimate ConfInt Double)
getIters r =
  Map.lookup "iters" . regCoeffs =<<
  find (\r -> regResponder r == "time") (anRegress (reportAnalysis r))

data PlotData = PlotData
  { plotName :: String
  , lines :: [LineData]
  } deriving Show

data LineData = LineData
  { lineName :: String
  , linePoints :: [(Double, Estimate ConfInt Double)]
  } deriving Show

aggregate :: [Result] -> [PlotData]
aggregate rs =
  (map (uncurry PlotData) .
   Map.toList . Map.map (map (uncurry toLineData) . Map.toList))
    byImpl
  where
    byOp :: Map String (NonEmpty Result)
    byOp = aggregateBy operation rs
    byImpl :: Map String (Map String (NonEmpty Result))
    byImpl = Map.map (aggregateBy implementation . toList) byOp
    toLineData :: String -> NonEmpty Result -> LineData
    toLineData k rs = LineData k (map (\r -> (size r, iters r)) (toList rs))

aggregateBy :: Ord k => (a -> k) -> [a] -> Map k (NonEmpty a)
aggregateBy f = Map.fromListWith (<>) . map (\x -> (f x, x :| []))

generatePlot :: PlotData -> EC (Layout Double Double) ()
generatePlot (PlotData n ls) = do
  layout_title .= n
  for_ ls $ \(LineData n' ps) -> do
    let ps' = map (\(x, e) -> (LogValue x, estPoint e)) ps
    plot (line n' [ps'])
    plot (points n' ps')
