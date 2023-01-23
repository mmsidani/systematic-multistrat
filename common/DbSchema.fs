module SA2.Common.DbSchema

open System
open System.Data
open System.Data.Linq
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq





let dbMaxParametersConstant = 2000 // the maximum number of parameters SQL server can handle


// dev / prod logic

let mutable useProductionDatabase = true




type MultistrategySchema = DbmlFile< "sa2_multistrategy.dbml" >
type Sa2MarketDataSchema = DbmlFile< "sa2_market_data.dbml" >



type DatabaseFields =

    static member marketCap = "marketCap"

    static member price = "price"

    static member salesPerShare = "salesPerShare"

    static member sharesOutstanding = "sharesOutstanding"

    static member margin = "grossMargin"

    static member ebitda = "ebitda"

    static member ebit = "ebit"

    static member bookValue = "bookValue"

    static member enterpriseValue = "enterpriseValue"

    static member longTermBorrowing = "ltDebt"

    static member totalEquity = "totalEquity"

    static member priceToBook = "priceToBook"

    static member operatingIncome = "operatingIncome"

    static member returnOnEquity = "returnOnEquity"

    static member returnOnCapital = "returnOnCapital"

    static member earnings = "earnings"

    static member dividendYield = "dividentYield"

    static member dailyTotalReturn = "dtr"

    static member currency = "currency"

    static member gicsIndustry = "gicsIndustry"

    static member shortName = "shortName"

    static member fundamentalTicker = "fundamentalTicker"

    static member gdpQoq = "GdpQoq"
    
    static member yld = "yield"

    static member nominalGrowthRate = "GdpQoq"

    static member realGrowthRate = "RealGdpRate"

    static member cpiRate = "cpiRate"

    static member baseCurrency = "baseCurrency"

    static member bid = "bid"

    static member ask = "ask"

    static member mid = "mid"

    static member dayCount = "dayCount"

    static member tenor = "tenor"

    static member daysToSettle = "daysToSettle"

    static member fixedPayFreq = "fixedPayFreq"

    static member floatIndex = "floatIndex"

    static member floatResetFreq = "floatResetFreq"

    static member floatDayCount = "floatDayCount"

    static member oisFloatPayDelay = "oisFloatPayDelay"

    static member floatPayFreq = "floatPayFreq"




let createMultistrategyContext () = 
    
    if useProductionDatabase then
    
        new MultistrategySchema.Sa2_multistrategy( "Data Source=SOL;Initial Catalog=sa2_multistrategy;User ID=SA2Trader;Password=31July!;" ) 

    else 

        new MultistrategySchema.Sa2_multistrategy( "Data Source=TERRA;Initial Catalog=sa2_multistrategy;User ID=SA2Trader;Password=31July!;" )




let createSa2MarketDataContext () =

    if useProductionDatabase then
    
        new Sa2MarketDataSchema.Sa2_market_data( "Data Source=SOL;Initial Catalog=sa2_market_data;User ID=SA2Trader;Password=31July!;" )

    else 

        new Sa2MarketDataSchema.Sa2_market_data( "Data Source=TERRA;Initial Catalog=sa2_market_data;User ID=SA2Trader;Password=31July!;" )