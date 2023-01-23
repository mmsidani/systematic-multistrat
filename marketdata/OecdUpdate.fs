module SA2.MarketData.OecdUpdate

open System

open SA2.Common.Table_market_data
open SA2.Common.Io
open SA2.Common.Dates
open SA2.Common.Utils
open SA2.Common.DbSchema


type OecdInputType = {

    gdpNominalQoqSeriesName : string

    gdpNominalQoqSeriesDescription : string

    gdpNominalQoqSeriesFrequency : string

    gdpQoqFileName : string

    countryInstrumentMap : Map< string , string >

}



let oecdQuarterToDate periods =

    let quarterToMonth = [ ( "Q1" , "0331" ) ; ( "Q2" , "0630" ) ; ( "Q3" , "0930" ) ; ( "Q4" , "1231" ) ] |> Map.ofList

    let dates =  List.map ( fun ( p : string ) -> 
    
                                let qs = p.Split( [| '-' |] ) 
                                
                                DateTime.ParseExact ( qs.[ 1 ] + ( Map.find qs.[ 0 ] quarterToMonth ) , "yyyyMMdd" , null ) ) periods

    dates |> shiftWeekends |> List.map ( fun d -> obj2Date d )




let private germanySpecial data =

    // "Former Federal Republic of Germany" and "Germany" overlap in 1991; we take the latter

    data 
    
        |> Array.filter ( fun ( s : string[] ) -> ( s.[ 3 ].Contains( "Former Federal Republic of Germany" ) && s.[ 4 ].Contains( "1991" ) ) |> not )

        |> Array.map ( 
        
                    fun ( s : string[] ) -> 

                        if s.[ 3 ].Contains( "Former Federal Republic of Germany" ) then

                            s.[ 3 ] <- "Germany"

                        s
                    )




let updateGdpCore countryInstrumentMap dataFileName seriesName seriesDescription seriesFrequency =

    let field = DatabaseFields.gdpQoq

    let source = "OECD"

    let data = readFileRemoveHeader dataFileName

                |> List.map ( fun line -> line.Split( [| ',' |] ) )

                |> Array.ofSeq

                |> germanySpecial
    
    let seriesNames , seriesDescriptions , seriesFrequencies = data |> Array.map ( fun s -> ( s.[ 0 ] , s.[ 1 ] , s.[ 2 ] ) ) |> Array.unzip3
    
    if ( set seriesNames ).Count <> 1 || ( set seriesDescriptions ).Count <> 1 || ( set seriesFrequencies ).Count <> 1 then

        raise( Exception "more than one series or inconsistent descriptions or more than 1 frequency" )

    if ( seriesNames.[0] <> seriesName || seriesDescriptions.[0] <> seriesDescription || seriesFrequencies.[0] <> seriesFrequency ) then
    
        raise( Exception "data file does not match specification" )

    let countriesPeriodsValues = 
        
        let names , sq =

            data 
        
            |> Array.map ( fun s -> ( s.[ 3 ] , ( s.[ 3 ] , ( [ s.[ 4 ] ] |> oecdQuarterToDate |> Seq.head , s.[ 5 ] ) ) ) ) 

            |> Array.unzip

        sq

            |> buildMap2List

            |> Map.map ( 
        
                        fun _ l -> 
        
                            l 
                        
                                |> List.sortBy ( fun ( p , _ ) -> p ) 
                        
                                |> Seq.pairwise 
                        
                                |> Seq.map ( fun ( ( _ , v1 ) , ( p , v2 ) ) -> if float v1 <> 0.0 then Some( ( p , float v2 / float v1 - 1.0 ) ) else None )

                                |> Seq.filter ( fun pv -> pv.IsSome )

                                |> Seq.map ( fun pv -> pv.Value )

                                |> Array.ofSeq

                        )
        
            |> Map.toArray

            |> Array.map ( fun ( k , l ) -> l |> Array.map ( fun ( p , v ) -> ( k , p , v ) ) )

            |> Array.concat
    
    try

        countriesPeriodsValues

            |> Array.map ( fun ( c , p , v ) -> ( p , seriesNames.[ 0 ] , Map.find c countryInstrumentMap , v ) )

            |> update_market_data source field field 1.0

    with

        | e -> raise( e )
 



let updateGdp ( oecdInput : OecdInputType ) =

    try

        updateGdpCore oecdInput.countryInstrumentMap oecdInput.gdpQoqFileName oecdInput.gdpNominalQoqSeriesName oecdInput.gdpNominalQoqSeriesDescription oecdInput.gdpNominalQoqSeriesFrequency

    with

        | e -> e.ToString() |> printfn "error updating gdp from OECD: %s" 