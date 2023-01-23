#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"


#load ".\settingsProduction.fsx"
#load ".\settingsFxConversion.fsx"


open SA2.Equity.IndexesPortfolio
open SA2.Common.Dates
open SA2.Common.DbSchema




let private ratesPortfolioInput = {

    numeraire = "us.gg.3m"

    fxConversionRates = SettingsFxConversion.fxConversionRates

    growthDifferential = 
    
        [

            { rateToTrade = "AS51 INDEX" ; proxyRate = "au.gg.2y" ; growthRate = "AUGDPCQ Index" ; conversionRate = "au.gg.3m" ; realGdpRate = "au.gdprate" } ;
            { rateToTrade = "SPTSX60 INDEX" ; proxyRate = "ca.gg.2y" ; growthRate = "CGEBQOQ Index" ; conversionRate = "ca.gg.3m" ; realGdpRate = "ca.gdprate" } ;
            { rateToTrade = "SMI INDEX" ; proxyRate = "sz.gg.2y" ; growthRate = "SZGNGDPQ Index" ; conversionRate = "sz.gg.3m" ; realGdpRate = "sz.gdprate" } ;
            { rateToTrade = "IBEX INDEX" ; proxyRate = "sp.gg.2y" ; growthRate = "SPNAGDPG Index" ; conversionRate = "sp.gg.3m" ; realGdpRate = "sp.gdprate" } ;
            { rateToTrade = "DAX INDEX" ; proxyRate = "ge.gg.2y" ; growthRate = "GDPBGDQQ Index" ; conversionRate = "ge.gg.3m" ; realGdpRate = "ge.gdprate" } ;
            { rateToTrade = "CAC INDEX" ; proxyRate = "fr.gg.2y" ; growthRate = "FRNGGDPQ Index" ; conversionRate = "fr.gg.3m" ; realGdpRate = "fr.gdprate" } ;
            { rateToTrade = "NIFTY INDEX" ; proxyRate = "in.gg.2y" ; growthRate = "INXQGDPM Index" ; conversionRate = "in.gg.3m" ; realGdpRate = "in.gdprate" } ;
            { rateToTrade = "FTSEMIB INDEX" ; proxyRate = "it.gg.2y" ; growthRate = "ITPINLQS Index" ; conversionRate = "it.gg.3m" ; realGdpRate = "it.gdprate" } ;
            { rateToTrade = "TPX INDEX" ; proxyRate = "jp.gg.2y" ; growthRate = "JGDOQOQ Index" ; conversionRate = "jp.gg.3m" ; realGdpRate = "jp.gdprate" } ;
            { rateToTrade = "KOSPI2 INDEX" ; proxyRate = "ko.gg.2y" ; growthRate = "KOEGSTOQ Index" ; conversionRate = "ko.gg.3m" ; realGdpRate = "ko.gdprate" } ;
            { rateToTrade = "MEXBOL INDEX" ; proxyRate = "mx.gg.2y" ; growthRate = "MXNPSUNQ Index" ; conversionRate = "mx.gg.3m" ; realGdpRate = "mx.gdprate" } ;
            { rateToTrade = "OMX INDEX" ; proxyRate = "sw.gg.2y" ; growthRate = "SWGCGDPQ Index" ; conversionRate = "sw.gg.3m" ; realGdpRate = "sw.gdprate" } ;
            { rateToTrade = "UKX INDEX" ; proxyRate = "uk.gg.2y" ; growthRate = "UKGRYBAQ Index" ; conversionRate = "uk.gg.3m" ; realGdpRate = "uk.gdprate" } ;
            { rateToTrade = "INDU INDEX" ; proxyRate = "us.gg.2y" ; growthRate = "GDP CUAQ Index" ; conversionRate = "us.gg.3m" ; realGdpRate = "us.gdprate" } ;
            { rateToTrade = "TOP40 INDEX" ; proxyRate = "sa.gg.2y" ; growthRate = "SADXGDEN Index" ; conversionRate = "sa.gg.3m" ; realGdpRate = "sa.gdprate" } ;
//            { rateToTrade = "SX5E INDEX" ; proxyRate = "eu.gg.2y" ; growthRate = "ez.nomGdp" ; conversionRate = "eu.gg.3m" ; realGdpRate = "eu.gdprate" } ;
            { rateToTrade = "NKY INDEX" ; proxyRate = "jp.gg.2y" ; growthRate = "JGDOQOQ Index" ; conversionRate = "jp.gg.3m" ; realGdpRate = "jp.gdprate" } ;            
            { rateToTrade = "SPX INDEX" ; proxyRate = "us.gg.2y" ; growthRate = "GDP CUAQ Index" ; conversionRate = "us.gg.3m" ; realGdpRate = "us.gdprate" } ;            
            { rateToTrade = "NDX INDEX" ; proxyRate = "us.gg.2y" ; growthRate = "GDP CUAQ Index" ; conversionRate = "us.gg.3m" ; realGdpRate = "us.gdprate" } ;            
            { rateToTrade = "RTY INDEX" ; proxyRate = "us.gg.2y" ; growthRate = "GDP CUAQ Index" ; conversionRate = "us.gg.3m" ; realGdpRate = "us.gdprate" } ;           
            { rateToTrade = "AEX INDEX" ; proxyRate = "nl.gg.2y" ; growthRate = "NEGDPSCQ Index" ; conversionRate = "nl.gg.3m" ; realGdpRate = "nl.gdprate" } ;          
            { rateToTrade = "OBXP INDEX" ; proxyRate = "no.gg.2y" ; growthRate = "NOGDPCH Index" ; conversionRate = "no.gg.3m" ; realGdpRate = "no.gdprate" } ;          
            { rateToTrade = "WIG20 INDEX" ; proxyRate = "pl.gg.2y" ; growthRate = "PODPQOQ Index" ; conversionRate = "pl.gg.3m" ; realGdpRate = "pl.gdprate" } ;         
            { rateToTrade = "RTSI$ INDEX" ; proxyRate = "ru.gg.2y" ; growthRate = "RUDPGLQQ Index" ; conversionRate = "ru.gg.3m" ; realGdpRate = "ru.gdprate" } ;          
            { rateToTrade = "IBOV INDEX" ; proxyRate = "bz.gg.2y" ; growthRate = "BZGDGDPQ Index" ; conversionRate = "bz.gg.3m" ; realGdpRate = "bz.gdprate" } ;          
            { rateToTrade = "XU030 INDEX" ; proxyRate = "tk.gg.2y" ; growthRate = "TUGPCUQQ Index" ; conversionRate = "tk.gg.3m" ; realGdpRate = "tk.gdprate" } ;          
            { rateToTrade = "HSI INDEX" ; proxyRate = "hk.gg.2y" ; growthRate = "HKGCGDP Index" ; conversionRate = "hk.gg.3m" ; realGdpRate = "hk.gdprate" } ;          
            { rateToTrade = "SET50 INDEX" ; proxyRate = "th.gg.2y" ; growthRate = "THG PC$Q Index" ; conversionRate = "th.gg.3m" ; realGdpRate = "th.gdprate" } ;      
//            { rateToTrade = "TAMSCI INDEX" ; proxyRate = "tw.gg.2y" ; growthRate = "TWRGSANQ Index" ; conversionRate = "tw.gg.3m" ; realGdpRate = "tw.gdprate" } ;     
            { rateToTrade = "FBMKLCI INDEX" ; proxyRate = "my.gg.2y" ; growthRate = "MAGRTTQQ Index" ; conversionRate = "my.gg.3m" ; realGdpRate = "my.gdprate" } ;     
            { rateToTrade = "SIMSCI INDEX" ; proxyRate = "sg.gg.2y" ; growthRate = "SGDPCURQ Index" ; conversionRate = "sg.gg.3m" ; realGdpRate = "sg.gdprate" } ;
            
        ] ;

    rateField = DatabaseFields.yld ;

    proxyRateField = DatabaseFields.yld ;

    growthField = DatabaseFields.price ; /// DatabaseFields.nominalGrowthRate ;

    conversionRateField = DatabaseFields.yld ;

    gdpField = DatabaseFields.realGrowthRate ;

    rateDays = 1 ; 

    proxyDays = 60 ;

    growthQuarters = 1 ;

    gapQuarters = 2 ;

    lambda = 1600.0 ;

    longPositionsFraction = 0.5 ;

    outputFile = "../output/indexesStrategyOutput.csv"

}




let private ratesPortfolioHistoricalInput =

    {
        
        ratesPortfolioInput = ratesPortfolioInput ;

        startDate = 20030101 ;

        endDate = 20130930 ;

        frequency = DateFrequency.weekly 

    }



// execute

buildIndexesPortfolioHistorical ratesPortfolioHistoricalInput

