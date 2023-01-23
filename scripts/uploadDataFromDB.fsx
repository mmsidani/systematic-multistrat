#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\FSharp\3.0\Runtime\v4.0\Type Providers\FSharp.Data.TypeProviders.dll"

open System

open System
open System.Linq
open Microsoft.FSharp.Data.TypeProviders


open SA2.Common.Dictionaries
open SA2.Common.Table_market_data
open SA2.Common.Dates




type TransferDataInput = {

    tickerSa2Label : ( string * string ) list

    field : string

    source: string

}



type dbSchemaSa2MarketData = DbmlFile< "..\common\sa2_market_data.dbml" >
type dbSchemaSa2Multistrategy = DbmlFile< "..\common\sa2_multistrategy.dbml" >


let transferDataInput = {

    tickerSa2Label = [
                    (*
                        ( "GSWISS02 INDEX" , "sz.gg.2y" ) ; 
                        ( "USGG2YR INDEX" , "us.gg.2y" ) ; 
                        ( "GCAN2YR INDEX" , "ca.gg.2y" ) ; 
                        ( "GUKG2 INDEX" , "uk.gg.2y" ) ; 
                        ( "GJGB2 INDEX" , "jp.gg.2y" ) ; 
                        ( "GACGB2 INDEX" , "au.gg.2y" ) ;
                        ( "GFRN2 INDEX" , "fr.gg.2y" ) ; 
                        ( "GSGB2YR INDEX" , "sw.gg.2y" ) ; 
                        ( "GDBR2 INDEX" , "eu.gg.2y" ) ; 
                        ( "HKGG2Y INDEX" , "hk.gg.2y" ) ; 
                        ( "MASB2Y INDEX" , "sg.gg.2y" ) ; 
                        ( "GBTPGR2 INDEX" , "it.gg.2y" ) ; 
                        ( "GSPG2YR INDEX" , "sp.gg.2y" ) ; 
                        ( "GNTH2YR INDEX" , "nl.gg.2y" ) ; 
                        ( "GSAB2YR INDEX" , "sa.gg.2y" ) ; 
                        ( "GVSK2YR INDEX" , "ko.gg.2y" ) ; 
                        ( "GCNY2YR INDEX" , "ch.gg.2y" ) ; 
                        ( "GMXN02YR INDEX" , "mx.gg.2y" ) ; 
                        ( "BZAD2Y INDEX" , "bz.gg.2y" ) ; 
                        ( "MICXRU2Y INDEX" , "ru.gg.2y" ) ; 
                        ( "POGB2YR INDEX" , "pl.gg.2y" ) ; 
                        ( "GSPT2YR INDEX" , "pt.gg.2y" ) ; 
                        ( "GVTL2YR INDEX" , "th.gg.2y" ) ; 
                        ( "MAGY3YR INDEX" , "my.gg.2y" ) ; 
                        ( "GIDN2YR INDEX" , "id.gg.2y" ) ; 
                        ( "GIND1YR INDEX" , "in.gg.2y" ) ; 
                        ( "GVTW2YR INDEX" , "tw.gg.2y" ) ; 
                        ( "PDSF2YR INDEX" , "ph.gg.2y" ) ; 
                        ( "USGG10YR INDEX" , "us.gg.10y" ) ; 
                        ( "GCAN10YR INDEX" , "ca.gg.10y" ) ;
                        ( "GJGC10 INDEX" , "jp.gg.10y" ) ; 
                        ( "GUKG10 INDEX" , "uk.gg.10y" ) ;
                        ( "GFRN10 INDEX" , "fr.gg.10y" ) ; 
                        ( "GACGB10 INDEX" , "au.gg.10y" ) ; 
                        ( "GSGB10YR INDEX" , "sw.gg.10y" ) ; 
                        ( "GSWISS10 INDEX" , "sz.gg.10y" ) ; 
                        ( "GDBR10 INDEX" , "eu.gg.10y" ) ; 
                        ( "HKGG10Y INDEX" , "hk.gg.10y" ) ; 
                        ( "MASB10Y INDEX" , "sg.gg.10y" ) ; 
                        ( "GBTPGR10 INDEX" , "it.gg.10y" ) ; 
                        ( "GSPG10YR INDEX" , "sp.gg.10y" ) ; 
                        ( "GNTH10YR INDEX" , "nl.gg.10y" ) ; 
                        ( "GNOR10YR INDEX" , "no.gg.10y" ) ; 
                        ( "GNZGB10 INDEX" , "nz.gg.10y" ) ; 
                        ( "GSAB9YR INDEX" , "sa.gg.10y" ) ; 
                        ( "GVSK10YR INDEX" , "ko.gg.10y" ) ; 
                        ( "GCNY10YR INDEX" , "ch.gg.10y" ) ; 
                        ( "GMXN10YR INDEX" , "mx.gg.10y" ) ; 
                        ( "MICXRU10 INDEX" , "ru.gg.10y" ) ; 
                        ( "GGGB10YR INDEX" , "gr.gg.10y" ) ; 
                        ( "POGB10YR INDEX" , "pl.gg.10y" ) ; 
                        ( "GSPT10YR INDEX" , "pt.gg.10y" ) ; 
                        ( "GVTL10YR INDEX" , "th.gg.10y" ) ; 
                        ( "MAGY10YR INDEX" , "my.gg.10y" ) ; 
                        ( "GIDN10YR INDEX" , "id.gg.10y" ) ; 
                        ( "GIND10YR INDEX" , "in.gg.10y" ) ; 
                        ( "GVTW10YR INDEX" , "tw.gg.10y" ) ; 
                        ( "PDSF10YR INDEX" , "ph.gg.10y" ) ; 
                        ( "GIDN1YR INDEX" , "id.gg.3m" ) ;
                        ("GACGB1 INDEX","au.gg.3m") ;
                        ("GCAN3M INDEX","ca.gg.3m") ;
                        ("SF0003M INDEX","sz.gg.3m") ;
                        ("GSGLT3MO INDEX","sp.gg.3m") ;
                        ("GETB1 INDEX" , "ge.gg.3m") ;
                        ("GBTF3MO INDEX","fr.gg.3m") ;
                        ("GINTB3MO INDEX","in.gg.3m") ;
                        ("GBOTG3M INDEX","it.gg.3m") ;
                        ("GJTB3MO INDEX","jp.gg.3m") ;
                        ("GVSK3MON INDEX","ko.gg.3m") ;
                        ("MPTBC INDEX","mx.gg.3m") ;
                        ("GTBN3M INDEX","nl.gg.3m") ;
                        ("WIBO3M INDEX","pl.gg.3m") ;
                        ("MICXRU3M INDEX","ru.gg.3m") ;
                        ("GSGT3M INDEX","sw.gg.3m") ; 
                        ("UKGTB3M INDEX","uk.gg.3m") ;
                        ("USGG3M INDEX","us.gg.3m") ;
                        ("JIBA3M INDEX","sa.gg.3m") ;
                    *)

                        ( "CNBI3MO INDEX" , "ch.gg.3m" ) ;
                        ( "BNNN3M INDEX" , "my.gg.3m" ) ;
                        ( "TBDC3M INDEX" , "th.gg.3m" )

                     ] ;

    field = "yield" ;

    source = "tbl_rate_data" ;

}




let fieldDictionary = {

    bbField2Sa2NameDivisor =

        [
            ( "PX_LAST" , "yield" , 1.0 ) // yields in old DB table already divided by 100.0

        ]


}




let transferMarketData2Multistrategy ( input : TransferDataInput ) ( fieldDictionary : DataFieldDictionary ) =
    
    let source = new dbSchemaSa2MarketData.Sa2_market_data( "Data Source=SOL;Initial Catalog=sa2_market_data;User ID=SA2Trader;Password=31July!;" )

    let dest = new dbSchemaSa2Multistrategy.Sa2_multistrategy( "Data Source=TERRA;Initial Catalog=sa2_multistrategy;User ID=SA2Trader;Password=31July!;" )

    let ( tickers , descrs ) = input.tickerSa2Label |> List.unzip

    let descr2Ticker = ( descrs , tickers  ) ||> List.zip |> Map.ofList
    let ticker2Descr = ( tickers , descrs ) ||> List.zip |> Map.ofList

    let oldHeader =

        query {

            for row in source.Tbl_rate_header do
            where ( descrs.Contains row.Descr )
            select (  row.Rate_Id , row.Descr )

        }

    
    let rateIds = oldHeader |> Seq.map ( fun ( i , _ ) -> i ) |> Seq.toList

    let oldMap = Map.ofSeq oldHeader
       
    let dateDescrTickerValues = 
        
        query {

            for row in source.Tbl_rate_data do 
            where ( rateIds.Contains row.Rate_Id )
            select ( row.Date , row.Rate_Id , row.Rate )

        }

        |> Seq.toArray
        |> Array.filter ( fun ( _ , _ , r ) -> r.HasValue )
        |> Array.map ( 
            fun ( d , i , r ) ->         
                let descr = Map.find i oldMap
                ( d |> obj2Date , descr, Map.find descr descr2Ticker , r.Value ) )
    
    try

        update_market_data input.source input.field ( fieldDictionary.mapIt input.field ) ( fieldDictionary.divisor input.field ) dateDescrTickerValues

    with

        | e -> e.ToString() |> printfn "%s" 




//execute

transferMarketData2Multistrategy transferDataInput fieldDictionary