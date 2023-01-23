module SA2.Common.Table_instruments

open System
open System.Linq

open DbSchema
open Utils




let updateWithSa2Symbol_instruments ( tickers2Descr : Map< string , string > ) ( tickers2Symbol : Map< string , string > ) =

    let multistrategyContext = createMultistrategyContext()

    let tickers = tickers2Descr |> keySet
    let tickers2 = tickers2Symbol |> keySet

    if tickers.Equals tickers2 |> not then

        raise ( Exception "tickers2Descr and tickers2Symbol do not have the same names")

    let tickers = Set.toList tickers
                
    let existingNames =

        query {
            
            for row in multistrategyContext.Tbl_instruments do

            select ( row.InstrumentTicker , row )

        }

        |> Map.ofSeq

    tickers // deal with modified records

        |> List.filter ( fun n -> existingNames.ContainsKey n ) 
        |> List.filter ( 
        
            fun n -> 
            
                let row = Map.find n existingNames 
                
                row.Description <> Map.find n tickers2Descr || row.Sa2Symbol <> Map.find n tickers2Symbol
                
                )

        |> List.iter ( 
        
            fun n -> 

                let row = Map.find n existingNames
                row.Description <- Map.find n tickers2Descr
                row.Sa2Symbol <- Map.find n tickers2Symbol

                )             
    
    let newRecords =

        let newNames = tickers |> List.filter ( fun n -> existingNames.ContainsKey n |> not ) 

        let numNewNames = newNames.Length 

        if numNewNames <> 0 then
        
            let ids = 

                query {

                    for row in multistrategyContext.Tbl_instruments do

                    select row.InstrumentId

                } 
 
            let maxExistingId =
            
                match Seq.isEmpty ids with

                | true -> 0
                | false -> Seq.reduce ( fun i1 i2 -> max i1 i2 ) ids

            let newIds = [ 1 .. numNewNames ] |> List.map ( fun i -> i + maxExistingId )

            ( newNames , newIds ) 
                
                ||> Seq.map2 ( fun n i -> new MultistrategySchema.Tbl_instruments( InstrumentId = i , InstrumentTicker = n , Sa2Symbol = Map.find n tickers2Symbol , Description = Map.find n tickers2Descr ) )
                |> multistrategyContext.Tbl_instruments.InsertAllOnSubmit

            ( newNames , newIds )                                  

        else

            ( List.empty , List.empty )
        
    try

        multistrategyContext.SubmitChanges()
            
        newRecords

    with

    | e -> raise(e)        




let update_instruments ( tickers2Descr : Map< string , string > ) = 
    
    // Note: Sa2Symbol = InstrumentTicker in this one

    updateWithSa2Symbol_instruments tickers2Descr tickers2Descr
    



let getKeyFromSymbol_instruments ( instrumentSymbols : string Set ) =

    let multistrategyContext = createMultistrategyContext()
   
    let instrumentsTable = multistrategyContext.Tbl_instruments
                        
    let instrumentIds = 

        query {

            for row in instrumentsTable do

            select ( row.Sa2Symbol , row.InstrumentId )
        }

        |> Seq.filter ( fun ( s , _ ) -> instrumentSymbols.Contains s )

        |> Map.ofSeq


    let newNames = instrumentSymbols |> Set.filter ( fun n -> not ( instrumentIds |> Map.containsKey n ) )

    if ( newNames.Count <> 0 ) then
        
        raise ( Exception ( newNames |> paste ) )

    else

        instrumentIds




let getTickerFromSymbol_instruments ( instrumentSymbols : string Set ) =

    let multistrategyContext = createMultistrategyContext()
   
    let instrumentsTable = multistrategyContext.Tbl_instruments
                        
    let instrumentTickers = 

        query {

            for row in instrumentsTable do

            select ( row.Sa2Symbol , row.InstrumentTicker )
        }

        |> Seq.filter ( fun ( s , _ ) -> instrumentSymbols.Contains s )

        |> Map.ofSeq


    let newNames = instrumentSymbols |> Set.filter ( fun n ->  Map.containsKey n instrumentTickers |> not ) 

    if ( newNames.Count <> 0 ) then
        
        raise ( Exception ( newNames |> paste ) )

    else

        instrumentTickers




let getSymbolFromTicker_instruments ( instrumentTickers : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let instrumentsTable = multistrategyContext.Tbl_instruments
                        
    let instrumentSymbols = 

        query {

            for row in instrumentsTable do

            select ( row.InstrumentTicker , row.Sa2Symbol )
        }

        |> Seq.filter ( fun ( t , _ ) -> instrumentTickers.Contains t )

        |> Map.ofSeq


    let newNames = instrumentTickers |> Set.filter ( fun n -> not ( instrumentSymbols |> Map.containsKey n ) )

    if ( newNames.Count <> 0 ) then
        
        raise ( KeysNotFound ( newNames ) )

    else

        instrumentSymbols



let getSymbolFromKey_instruments ( instrumentIds : int Set ) =
    
    let multistrategyContext = createMultistrategyContext()

    let instrumentsTable = multistrategyContext.Tbl_instruments

    let instrumentSymbols = 

        query {

            for row in instrumentsTable do

            select ( row.InstrumentId , row.Sa2Symbol )
        }

        |> Seq.filter ( fun ( i , _ ) -> instrumentIds.Contains i )

        |> Map.ofSeq


    let newIds = instrumentIds |> Set.filter ( fun n -> not ( instrumentSymbols |> Map.containsKey n ) )

    if ( newIds.Count <> 0 ) then

        raise ( KeysNotFound ( newIds |> Set.map ( fun i -> i.ToString() ) ) )

    else

        instrumentSymbols




let getKey_instruments ( instrumentTickers : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let instrumentsTable = multistrategyContext.Tbl_instruments
                        
    let instrumentIds = 

        query {

            for row in instrumentsTable do

            select ( row.InstrumentTicker , row.InstrumentId )
        }

        |> Seq.filter ( fun ( t , _ ) -> instrumentTickers.Contains t )

        |> Map.ofSeq


    let newNames = instrumentTickers |> Set.filter ( fun n -> not ( instrumentIds |> Map.containsKey n ) )

    if ( newNames.Count <> 0 ) then
        
        raise ( Exception ( newNames |> paste ) )

    else

        instrumentIds




let getDescription_instruments ( instrumentTickers : string Set ) =
    
    let multistrategyContext = createMultistrategyContext()
   
    let instrumentsTable = multistrategyContext.Tbl_instruments
                        
    let instrumentDescriptions = 

        query {

            for row in instrumentsTable do

            select ( row.InstrumentTicker , row.Description )
        }

        |> Seq.filter ( fun ( t , _ ) -> instrumentTickers.Contains t )

        |> Map.ofSeq


    let newNames = instrumentTickers |> Set.filter ( fun n -> not ( instrumentDescriptions |> Map.containsKey n ) )

    if ( newNames.Count <> 0 ) then
        
        raise ( KeysNotFound ( newNames ) )

    else

        instrumentDescriptions




let getTicker_instruments ( instrumentIds : int Set ) =
    
    let multistrategyContext = createMultistrategyContext()

    let instrumentsTable = multistrategyContext.Tbl_instruments

    let instrumentTickers = 

        query {

            for row in instrumentsTable do

            select ( row.InstrumentId , row.InstrumentTicker )
        }

        |> Seq.filter ( fun ( i , _ ) -> instrumentIds.Contains i )

        |> Map.ofSeq


    let newIds = instrumentIds |> Set.filter ( fun n -> not ( instrumentTickers |> Map.containsKey n ) )

    if ( newIds.Count <> 0 ) then

        raise ( KeysNotFound ( newIds |> Set.map ( fun i -> i.ToString() ) ) )

    else

        instrumentTickers




let getAllTickers_instruments () =
    
    let multistrategyContext = createMultistrategyContext()

    query {

        for row in multistrategyContext.Tbl_instruments do

        select ( row.InstrumentTicker )
    }

    |> List.ofSeq




let getAllDescriptions_instruments () =
    
    let multistrategyContext = createMultistrategyContext()

    query {

        for row in multistrategyContext.Tbl_instruments do

        select ( row.Description )
    }

    |> List.ofSeq