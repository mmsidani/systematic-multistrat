#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\rates.dll"


#load ".\settingsProduction.fsx"
#load ".\settingsFxConversion.fsx"


open SA2.Rates.RatesPortfolio
open SA2.Common.Dates
open SA2.Common.DbSchema




let private ratesPortfolioInput = {

    
    numeraire = "us.gg.3m" // conversion rate to be used to get back to domestic perspective

    rateTradingPackets = 
    
        [

            // rateToTrade can be from old table, e.g., us.gg.10y, or if a swap can be specified using either Bloomberg ticker or RiskEvo symbology. if using riskevo set useSa2SymbolForRate = true
            // proxyRate can be from old table, e.g., us.gg.2y, or if a swap rate or fixing index rate are used either Bloomberg ticker or RiskEvo symbology. if using riskevo set useSa2SymbolForProxy = true
            // .nomGdp names are OECD nominal gdp rates. to use the nominal QoQ gdp rates we downloaded from Bloomberg, the Bloomberg ticker should be used, e.g., "MXGPQTR INDEX" instead mx.nomGdp
            // conversionRates are used in converting foreign yields to domestic perspective
            // outputGapRate is typically real gdp yoy which we only have in old DB, tbl_sa2_macro_data table




//            { rateToTrade = "InterestRateSwap.AUD.BB.M.6.M.6.Y.10" ; proxyRate = "Deposit.AUD.AONIA.D.1" ; growthDifferentialRate = "au.nomGdp" ; conversionRate = "au.gg.3m" ; outputGapRate = "au.gdprate" ; cpiRate = "au.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.CAD.BA.M.3.M.6.Y.10" ; proxyRate = "Deposit.CAD.BA.M.3" ; growthDifferentialRate = "ca.nomGdp" ; conversionRate = "ca.gg.3m" ; outputGapRate = "ca.gdprate" ; cpiRate = "ca.cpirate"  } ;
//            { rateToTrade = "InterestRateSwap.CHF.LIBOR.M.6.M.12.Y.10" ; proxyRate = "Deposit.CHF.LIBOR.M.6" ; growthDifferentialRate = "ch.nomGdp" ; conversionRate = "sz.gg.3m" ; outputGapRate = "sz.gdprate" ; cpiRate = "sz.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.JPY.LIBOR.M.6.M.6.Y.10" ; proxyRate = "Deposit.JPY.LIBOR.M.6" ; growthDifferentialRate = "jp.nomGdp" ; conversionRate = "jp.gg.3m" ; outputGapRate = "jp.gdprate" ; cpiRate = "jp.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.SEK.STIBOR.M.3.M.12.Y.10" ; proxyRate = "Deposit.SEK.STINA.D.1" ; growthDifferentialRate = "se.nomGdp" ; conversionRate = "sw.gg.3m" ; outputGapRate = "sw.gdprate" ; cpiRate = "sw.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.GBP.LIBOR.M.6.M.6.Y.10" ; proxyRate = "Deposit.GBP.LIBOR.M.6" ; growthDifferentialRate = "uk.nomGdp" ; conversionRate = "uk.gg.3m" ; outputGapRate = "uk.gdprate" ; cpiRate = "uk.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.USD.LIBOR.M.3.M.12.Y.10|InterestRateSwap.USD.LIBOR.M.3.M.12.Y.1" ; proxyRate = "Deposit.USD.LIBOR.M.6" ; growthDifferentialRate = "us.nomGdp" ; conversionRate = "us.gg.3m" ; outputGapRate = "us.gdprate" ; cpiRate = "us.cpirate" } ;
//            { rateToTrade = "InterestRateSwap.ZAR.JIBOR.M.3.M.3.Y.10" ; proxyRate = "Deposit.ZAR.JIBOR.M.3" ; growthDifferentialRate = "za.nomGdp" ; conversionRate = "sa.gg.3m" ; outputGapRate = "sa.gdprate" ; cpiRate = "sa.cpirate" } ;

            { rateToTrade = "OISRateSwap.USD.FEDFUND.D.1.M.12.M.12.Y.10" ; proxyRate = "FEDL01 INDEX" ; growthDifferentialRate = "GDP CUAQ INDEX" ; conversionRate = "FEDL01 INDEX" ; outputGapRate = "us.gdprate" ; cpiRate = "us.cpirate" };
            { rateToTrade = "OISRateSwap.JPY.MUTAN.D.1.M.12.M.12.Y.10" ; proxyRate = "MUTSCALM INDEX" ; growthDifferentialRate = "JGDOQOQ INDEX" ; conversionRate = "MUTSCALM INDEX" ; outputGapRate = "jp.gdprate" ; cpiRate = "jp.cpirate" };
            { rateToTrade = "OISRateSwap.INR.NSERO.D.1.M.6.M.6.Y.10" ; proxyRate = "NSERO INDEX" ; growthDifferentialRate = "INXQGDPM INDEX" ; conversionRate = "NSERO INDEX" ; outputGapRate = "in.gdprate" ; cpiRate = "in.cpirate" };
            { rateToTrade = "OISRateSwap.INR.NSERO.D.1.M.6.M.6.Y.10" ; proxyRate = "NSERO INDEX" ; growthDifferentialRate = "INXQGDPM INDEX" ; conversionRate = "NSERO INDEX" ; outputGapRate = "in.gdprate" ; cpiRate = "in.cpirate" };
            { rateToTrade = "OISRateSwap.GBP.SONIA.D.1.M.12.M.12.Y.10" ; proxyRate = "SONIO/N INDEX" ; growthDifferentialRate = "UKGRYBAQ INDEX" ; conversionRate = "SONIO/N INDEX" ; outputGapRate = "uk.gdprate" ; cpiRate = "uk.cpirate" };
            { rateToTrade = "OISRateSwap.EUR.EONIA.D.1.M.12.M.12.Y.10" ; proxyRate = "EONIA INDEX" ; growthDifferentialRate = "EUGDEU27 INDEX" ; conversionRate = "EONIA INDEX" ; outputGapRate = "eu.gdprate" ; cpiRate = "eu.cpirate" };
            { rateToTrade = "OISRateSwap.CHF.TOIS.D.1.M.12.M.12.Y.10" ; proxyRate = "TOISTOIS INDEX" ; growthDifferentialRate = "SZGNGDPQ INDEX" ; conversionRate = "TOISTOIS INDEX" ; outputGapRate = "sz.gdprate" ; cpiRate = "sz.cpirate" };
            { rateToTrade = "OISRateSwap.CAD.CORRA.D.1.M.12.M.12.Y.10" ; proxyRate = "CAONREPO INDEX" ; growthDifferentialRate = "CGEBQOQ INDEX" ; conversionRate = "CAONREPO INDEX" ; outputGapRate = "ca.gdprate" ; cpiRate = "ca.cpirate" };
            { rateToTrade = "OISRateSwap.AUD.AONIA.D.1.M.12.M.12.Y.10" ; proxyRate = "RBACOR INDEX" ; growthDifferentialRate = "AUGDPCQ INDEX" ; conversionRate = "RBACOR INDEX" ; outputGapRate = "au.gdprate" ; cpiRate = "au.cpirate" };
            { rateToTrade = "InterestRateSwap.ZAR.JIBOR.M.3.M.3.Y.10" ; proxyRate = "JIBA3M INDEX" ; growthDifferentialRate = "SRQGGDPQ INDEX" ; conversionRate = "JIBA3M INDEX" ; outputGapRate = "sa.gdprate" ; cpiRate = "sa.cpirate" };
            { rateToTrade = "InterestRateSwap.VND.SOR.M.3.M.3.Y.10" ; proxyRate = "VIFOR3M INDEX" ; growthDifferentialRate = "VEGCTOTL INDEX" ; conversionRate = "VIFOR3M INDEX" ; outputGapRate = "vn.gdprate" ; cpiRate = "vn.cpirate" };
            { rateToTrade = "InterestRateSwap.USD.LIBOR.M.3.M.6.Y.10" ; proxyRate = "US0003M INDEX" ; growthDifferentialRate = "GDP CUAQ INDEX" ; conversionRate = "US0003M INDEX" ; outputGapRate = "us.gdprate" ; cpiRate = "us.cpirate" };
            { rateToTrade = "InterestRateSwap.USD.LIBOR.M.3.M.12.Y.10" ; proxyRate = "US0003M INDEX" ; growthDifferentialRate = "GDP CUAQ INDEX" ; conversionRate = "US0003M INDEX" ; outputGapRate = "us.gdprate" ; cpiRate = "us.cpirate" };
            { rateToTrade = "InterestRateSwap.TWD.TAIBIR.D.90.M.3.Y.10" ; proxyRate = "TDSF90D INDEX" ; growthDifferentialRate = "TWRGSANQ INDEX" ; conversionRate = "TDSF90D INDEX" ; outputGapRate = "tw.gdprate" ; cpiRate = "tw.cpirate" };
            { rateToTrade = "InterestRateSwap.TRY.TRLIBOR.M.3.M.12.Y.10" ; proxyRate = "TRLIB3M INDEX" ; growthDifferentialRate = "TUGPCUQQ INDEX" ; conversionRate = "TRLIB3M INDEX" ; outputGapRate = "tk.gdprate" ; cpiRate = "tk.cpirate" };
            { rateToTrade = "InterestRateSwap.THB.SOR.M.6.M.6.Y.10" ; proxyRate = "THFX6M INDEX" ; growthDifferentialRate = "THG PC$Q INDEX" ; conversionRate = "THFX6M INDEX" ; outputGapRate = "th.gdprate" ; cpiRate = "th.cpirate" };
            { rateToTrade = "InterestRateSwap.SGD.SOR.M.6.M.6.Y.10" ; proxyRate = "SORF6M INDEX" ; growthDifferentialRate = "SGDPCURQ INDEX" ; conversionRate = "SORF6M INDEX" ; outputGapRate = "sg.gdprate" ; cpiRate = "sg.cpirate" };
            { rateToTrade = "InterestRateSwap.SEK.STIBOR.M.3.M.12.Y.10" ; proxyRate = "STIB3M INDEX" ; growthDifferentialRate = "SWGCGDPQ INDEX" ; conversionRate = "STIB3M INDEX" ; outputGapRate = "sw.gdprate" ; cpiRate = "sw.cpirate" };
            { rateToTrade = "InterestRateSwap.SAR.SAIBOR.M.3.M.12.Y.10" ; proxyRate = "SAIB3M INDEX" ; growthDifferentialRate = "SRQGGDPQ INDEX" ; conversionRate = "SAIB3M INDEX" ; outputGapRate = "sa.gdprate" ; cpiRate = "sa.cpirate" };
            { rateToTrade = "InterestRateSwap.RUB.MOSKP.M.3.M.12.Y.10" ; proxyRate = "MOSKP3 INDEX" ; growthDifferentialRate = "RUDPGLQQ INDEX" ; conversionRate = "MOSKP3 INDEX" ; outputGapRate = "ru.gdprate" ; cpiRate = "ru.cpirate" };
            { rateToTrade = "InterestRateSwap.RON.ROBOR.M.6.M.12.Y.10" ; proxyRate = "BUBR3M INDEX" ; growthDifferentialRate = "RODPQOQ INDEX" ; conversionRate = "BUBR3M INDEX" ; outputGapRate = "ro.gdprate" ; cpiRate = "ro.cpirate" };
            { rateToTrade = "InterestRateSwap.PLP.PHIREF.M.3.M.3.Y.10" ; proxyRate = "PREF3MO INDEX" ; growthDifferentialRate = "PHGDPC$ INDEX" ; conversionRate = "PREF3MO INDEX" ; outputGapRate = "ph.gdprate" ; cpiRate = "ph.cpirate" };
            { rateToTrade = "InterestRateSwap.PLN.WIBOR.M.3.M.12.Y.10" ; proxyRate = "WIBR3M INDEX" ; growthDifferentialRate = "PODPQOQ INDEX" ; conversionRate = "WIBR3M INDEX" ; outputGapRate = "pl.gdprate" ; cpiRate = "pl.cpirate" };
            { rateToTrade = "InterestRateSwap.PKR.KIBOR.M.3.M.6.Y.10" ; proxyRate = "PKDP6M INDEX" ; growthDifferentialRate = "PAGCEXPN INDEX" ; conversionRate = "PKDP6M INDEX" ; outputGapRate = "pk.gdprate" ; cpiRate = "pk.cpirate" };
            { rateToTrade = "InterestRateSwap.NZD.BB.M.3.M.6.Y.10" ; proxyRate = "NFIX3FRA INDEX" ; growthDifferentialRate = "NZNTNOMQ INDEX" ; conversionRate = "NFIX3FRA INDEX" ; outputGapRate = "nz.gdprate" ; cpiRate = "nz.cpirate" };
            { rateToTrade = "InterestRateSwap.NZD.BB.M.3.M.6.Y.10" ; proxyRate = "NFIX3FRA INDEX" ; growthDifferentialRate = "NZNTNOMQ INDEX" ; conversionRate = "NFIX3FRA INDEX" ; outputGapRate = "nz.gdprate" ; cpiRate = "nz.cpirate" };
            { rateToTrade = "InterestRateSwap.NOK.NIBOR.M.6.M.12.Y.10" ; proxyRate = "NIBOR6M INDEX" ; growthDifferentialRate = "NOGDPCH INDEX" ; conversionRate = "NIBOR6M INDEX" ; outputGapRate = "no.gdprate" ; cpiRate = "no.cpirate" };
            { rateToTrade = "InterestRateSwap.MYR.KLIBOR.M.3.M.3.Y.10" ; proxyRate = "KLIB3M INDEX" ; growthDifferentialRate = "MAGRTTQQ INDEX" ; conversionRate = "KLIB3M INDEX" ; outputGapRate = "my.gdprate" ; cpiRate = "my.cpirate" };
            { rateToTrade = "InterestRateSwap.KWD.KIBOR.M.3.M.12.Y.10" ; proxyRate = "KIBOR3M INDEX" ; growthDifferentialRate = "KUCUTOTA INDEX" ; conversionRate = "KIBOR3M INDEX" ; outputGapRate = "kw.gdprate" ; cpiRate = "kw.cpirate" };
            { rateToTrade = "InterestRateSwap.KRW.CD.M.3.M.3.Y.10" ; proxyRate = "KWCDC CURNCY" ; growthDifferentialRate = "KOEGSTOQ INDEX" ; conversionRate = "KWCDC CURNCY" ; outputGapRate = "ko.gdprate" ; cpiRate = "ko.cpirate" };
            { rateToTrade = "InterestRateSwap.JPY.LIBOR.M.6.M.6.Y.10" ; proxyRate = "JY0006M INDEX" ; growthDifferentialRate = "JGDOQOQ INDEX" ; conversionRate = "JY0006M INDEX" ; outputGapRate = "jp.gdprate" ; cpiRate = "jp.cpirate" };
            { rateToTrade = "InterestRateSwap.INR.MIFOR.M.3.M.12.Y.10" ; proxyRate = "MIFORIM3 INDEX" ; growthDifferentialRate = "INXQGDPM INDEX" ; conversionRate = "MIFORIM3 INDEX" ; outputGapRate = "in.gdprate" ; cpiRate = "in.cpirate" };
            { rateToTrade = "InterestRateSwap.HUF.BUBOR.M.6.M.12.Y.10" ; proxyRate = "BUBOR06M INDEX" ; growthDifferentialRate = "HUGQT INDEX" ; conversionRate = "BUBOR06M INDEX" ; outputGapRate = "hu.gdprate" ; cpiRate = "hu.cpirate" };
            { rateToTrade = "InterestRateSwap.HKD.HIBOR.M.3.M.3.Y.10" ; proxyRate = "HIHD03M INDEX" ; growthDifferentialRate = "HKGCGDP INDEX" ; conversionRate = "HIHD03M INDEX" ; outputGapRate = "hk.gdprate" ; cpiRate = "hk.cpirate" };
            { rateToTrade = "InterestRateSwap.GBP.LIBOR.M.6.M.6.Y.10" ; proxyRate = "BP0006 INDEX" ; growthDifferentialRate = "UKGRYBAQ INDEX" ; conversionRate = "BP0006 INDEX" ; outputGapRate = "uk.gdprate" ; cpiRate = "uk.cpirate" };
            { rateToTrade = "InterestRateSwap.EUR.EURIBOR.M.6.M.12.Y.10" ; proxyRate = "EUR006M INDEX" ; growthDifferentialRate = "EUGDEU27 INDEX" ; conversionRate = "EUR006M INDEX" ; outputGapRate = "eu.gdprate" ; cpiRate = "eu.cpirate" };
            { rateToTrade = "InterestRateSwap.EUR.EURIBOR.M.3.M.12.Y.10" ; proxyRate = "EUR003M INDEX" ; growthDifferentialRate = "EUGDEU27 INDEX" ; conversionRate = "EUR003M INDEX" ; outputGapRate = "eu.gdprate" ; cpiRate = "eu.cpirate" };
            { rateToTrade = "InterestRateSwap.DKK.CIBOR.M.6.M.12.Y.10" ; proxyRate = "CIBO06M INDEX" ; growthDifferentialRate = "DEGDPCSQ INDEX" ; conversionRate = "CIBO06M INDEX" ; outputGapRate = "dk.gdprate" ; cpiRate = "dk.cpirate" };
            { rateToTrade = "InterestRateSwap.CZK.PRIBOR.M.3.M.12.Y.10" ; proxyRate = "PRIB03M INDEX" ; growthDifferentialRate = "CZGDPCSQ INDEX" ; conversionRate = "PRIB03M INDEX" ; outputGapRate = "cz.gdprate" ; cpiRate = "cz.cpirate" };
            { rateToTrade = "InterestRateSwap.COP.IBR.D.1.M.3.Y.10" ; proxyRate = "COOVIBR INDEX" ; growthDifferentialRate = "COCUPIBQ INDEX" ; conversionRate = "COOVIBR INDEX" ; outputGapRate = "co.gdprate" ; cpiRate = "co.cpirate" };
            { rateToTrade = "InterestRateSwap.CNY.SHIBOR.M.3.M.3.Y.10" ; proxyRate = "SHIF3M INDEX" ; growthDifferentialRate = "CNNGPC$ INDEX" ; conversionRate = "SHIF3M INDEX" ; outputGapRate = "ch.gdprate" ; cpiRate = "ch.cpirate" };
            { rateToTrade = "InterestRateSwap.CNY.REPO.D.7.M.3.Y.10" ; proxyRate = "CNRR007 INDEX" ; growthDifferentialRate = "CNNGPC$ INDEX" ; conversionRate = "CNRR007 INDEX" ; outputGapRate = "ch.gdprate" ; cpiRate = "ch.cpirate" };
            { rateToTrade = "InterestRateSwap.CLP.CLICR.D.1.M.6.Y.10" ; proxyRate = "CLICP INDEX" ; growthDifferentialRate = "CLGDQCUR INDEX" ; conversionRate = "CLICP INDEX" ; outputGapRate = "cl.gdprate" ; cpiRate = "cl.cpirate" };
            { rateToTrade = "InterestRateSwap.CHF.LIBOR.M.6.M.12.Y.10" ; proxyRate = "SF0006M INDEX" ; growthDifferentialRate = "SZGNGDPQ INDEX" ; conversionRate = "SF0006M INDEX" ; outputGapRate = "sz.gdprate" ; cpiRate = "sz.cpirate" };
            { rateToTrade = "InterestRateSwap.CAD.BA.M.3.M.6.Y.10" ; proxyRate = "CDOR03 INDEX" ; growthDifferentialRate = "CGEBQOQ INDEX" ; conversionRate = "CDOR03 INDEX" ; outputGapRate = "ca.gdprate" ; cpiRate = "ca.cpirate" };
            { rateToTrade = "InterestRateSwap.BHD.BHIBOR.M.3.M.12.Y.10" ; proxyRate = "BHIBOR3M INDEX" ; growthDifferentialRate = "BJQGDPQ INDEX" ; conversionRate = "BHIBOR3M INDEX" ; outputGapRate = "bh.gdprate" ; cpiRate = "bh.cpirate" };
            { rateToTrade = "InterestRateSwap.BGN.SOFIBOR.M.3.M.12.Y.10" ; proxyRate = "SOBR3M INDEX" ; growthDifferentialRate = "BUGQTOTL INDEX" ; conversionRate = "SOBR3M INDEX" ; outputGapRate = "bg.gdprate" ; cpiRate = "bg.cpirate" };
            { rateToTrade = "InterestRateSwap.AUD.BB.M.6.M.6.Y.10" ; proxyRate = "BBSW6M INDEX" ; growthDifferentialRate = "AUGDPCQ INDEX" ; conversionRate = "BBSW6M INDEX" ; outputGapRate = "au.gdprate" ; cpiRate = "au.cpirate" };
            { rateToTrade = "InterestRateSwap.AUD.BB.M.3.M.3.Y.10" ; proxyRate = "BBSW3M INDEX" ; growthDifferentialRate = "AUGDPCQ INDEX" ; conversionRate = "BBSW3M INDEX" ; outputGapRate = "au.gdprate" ; cpiRate = "au.cpirate" };
            { rateToTrade = "InterestRateSwap.AED.EMIBOR.M.3.M.12.Y.10" ; proxyRate = "EIBO3M INDEX" ; growthDifferentialRate = "UAGDGDPB INDEX" ; conversionRate = "EIBO3M INDEX" ; outputGapRate = "ae.gdprate" ; cpiRate = "ae.cpirate" };

        ] ;



    // "tbl_market_data" contains data in the sa2_multistrategy DB or "tbl_rate_data" contains data in the older sa2_market_data DB
    

    annualizeGrowthRate = true ; // needed when using nominal growth qoq because that rate is not annualized
    useReal = true ; // set true to calculate growth differential from real gdp yoy rates and real interest rates (real interest rate = nominal interest rate - cpi )

    rateField = DatabaseFields.mid ; // DatabaseFields.mid (or ask or bid) when using "tbl_market_data". doesn't matter for old db, i.e., tbl_rate_data
    rateTable = "tbl_market_data" ; // "tbl_rate_data" or "tbl_market_data"
    useSa2SymbolForRate = true ; // set this to true when using riskevo symbology for rates to trade otherwise false

    proxyRateField = DatabaseFields.price ; // set this to DatabaseFields.mid (or ask or bid) if using a swap rate as proxy (?) and DatabaseFields.price when using deposit. doesn't matter for old db, i.e., tbl_rate_data
    proxyRateTable = "tbl_market_data" ;// "tbl_rate_data" or "tbl_market_data"
    useSa2SymbolForProxy = false ; // set this to true when using riskevo symbology for proxy rates otherwise false

    growthDifferentialField = DatabaseFields.price ; // DatabaseFields.nominalGrowthRate when using OECD, i.e., cc.nomGdp, names; set to DatabaseFields.price when using BB data
    growthDifferentialTable = "tbl_market_data" ; // "tbl_market_data" only for now

    conversionRateField = DatabaseFields.price ; // set this to DatabaseFields.mid (or ask or bid) if using a swap rate for conversion (?) and DatabaseFields.price when using deposit. doesn't matter for old db, i.e., tbl_rate_data
    conversionRateTable = "tbl_market_data" ;

    gdpField = DatabaseFields.realGrowthRate ;
    gdpTable = "tbl_sa2_macro_data" ; //  "tbl_sa2_macro_data" . there's no real gdp data in new db yet

    cpiField = DatabaseFields.cpiRate ;
    cpiTable = "tbl_sa2_macro_data" ; // "tbl_sa2_macro_data" . there's no cpi data in new db yet

    rateDays = 1 ; // number of values to average rateToTrade

    proxyDays = 20 ; // number of values to average proxy rates

    growthQuarters = 4 ; // number of values to average growth in growth differential calculations

    gapQuarters = 4 ; // number of values to average growth in output gap calculations

    lambda = 1600.0 ; // don't touch

    longPositionsFraction = 0.5 ; // fraction of portfolio to go long

    outputFile = "../output/ratesStrategyOutput.csv" 

}




let private ratesPortfolioHistoricalInput =

    {
        
        ratesPortfolioInput = ratesPortfolioInput ;

        startDate = 20130930 ; /// 19770101 ;

        endDate = 20130930 ;

        frequency = DateFrequency.weekly 

    }



// execute

buildRatesPortfolioHistorical ratesPortfolioHistoricalInput