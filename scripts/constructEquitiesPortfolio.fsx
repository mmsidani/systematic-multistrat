#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"



#load ".\settingsProduction.fsx"
#load ".\settingsFxConversion.fsx"
#load ".\settingsIndexes.fsx"



open SA2.Common.Dates
open SA2.Common.DbSchema

open SA2.Equity.EquitiesPortfolio
open SA2.Equity.FeatureType


let equitiesPortfolioInput = 

    {

    marketDataFields = 

        [

//        DatabaseFields.margin ;

        DatabaseFields.salesPerShare ;

        DatabaseFields.sharesOutstanding ;

        DatabaseFields.ebitda;

        DatabaseFields.enterpriseValue ;

        DatabaseFields.price ;

        DatabaseFields.marketCap;

        DatabaseFields.bookValue ;

        DatabaseFields.longTermBorrowing ;

        DatabaseFields.priceToBook ;

//        DatabaseFields.totalEquity ;

        DatabaseFields.returnOnCapital ;

        DatabaseFields.operatingIncome

        ] ;

    staticDataFields =

        [

        DatabaseFields.currency ;

        DatabaseFields.gicsIndustry ;

        DatabaseFields.shortName 

        ] ;

    globalDataFields =

        [

        DatabaseFields.marketCap ;
        DatabaseFields.price ;
        DatabaseFields.salesPerShare

        ] ;

    forexIndicator = "CURNCY" ;

    baseCurrency = "USD" ;

    maxDataItems = 1000

    featureTypes = [ FeatureType.profitability ; FeatureType.evEbitda ] /// ; FeatureType.margin ; FeatureType.marketShare ]

    }



let indexesPortfolioInput =

    {

    indexNames = SettingsIndexes.investableIndexes.clusteredIndexes

    indexFields = [] ;

    model = IndexPortfolioModel.bestWorst ;

    underlyingPortfolio = equitiesPortfolioInput ;

    clusterModelNumberOfClusters = 5 ;

    clusterModelMininimumNamesPerCluster = 0 ;

    }



let indexesPortfolioHistoricalInput =

    {

    indexesPortfolioInput = indexesPortfolioInput

    startDate = 20030101

    endDate = 20131231 

    frequency = DateFrequency.weekly

    }



// execute

// equitiesPortfolio equitiesPortfolioInput SettingsFxConversion.fxConversionRates

let junkSw = new System.IO.StreamWriter ( "../output/percentageScore.csv" )
junkSw.Close()  // truncate it to 0 at the beginning of a new run

equityIndexesPortfolioHistorical indexesPortfolioHistoricalInput SettingsFxConversion.fxConversionRates SettingsIndexes.indexCaps 

