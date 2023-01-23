module SA2.Equity.EquitiesPortfolio

open System

open SA2.Common.ForEx
open SA2.Common.Table_market_data
open SA2.Common.Table_static_data
open SA2.Common.Table_equity_universe
open SA2.Common.Table_index_members
open SA2.Common.Dates
open SA2.Common.Utils
open SA2.Common.FxTypes
open SA2.Common.DbSchema
open SA2.Common.Returns




open RelativeShares
open MatrixModels
open BottomUpModels
open IndexWeights
open RiskModels
open IndexTypes
open FeatureType




type IndexPortfolioModel =

    | twoLevel = 0
    | clusters = 1
    | bestWorst = 2
    | bestWorstCluster = 3
    | lowCorreationCluster = 4




type EquitiesPortfolioInput = 

    {

        // names from equity universe DB

        marketDataFields : string list

        staticDataFields : string list

        globalDataFields : string list

        forexIndicator : string

        baseCurrency : string

        maxDataItems : int

        featureTypes : FeatureType list

    }




type IndexesPortfolioInput = 

    {
    
        indexNames : ( string list ) list

        indexFields : string list

        model : IndexPortfolioModel

        clusterModelNumberOfClusters : int

        clusterModelMininimumNamesPerCluster : int

        underlyingPortfolio : EquitiesPortfolioInput

    }




type EquitiesPortfolioHistoricalInput = 

    {

        equitiesPortfolioInput : EquitiesPortfolioInput

        startDate : int

        endDate : int

        frequency : DateFrequency

    }




type IndexesPortfolioHistoricalInput = 

    {

        indexesPortfolioInput : IndexesPortfolioInput

        startDate : int

        endDate : int

        frequency : DateFrequency

    }



let private updateMktDataInterval = 3 // in years




let getMarketData numRecords date tickers ( fields : string list ) =
   
    let mutable ret = Map.empty

    for field in fields do
        printfn "now doing %s %s" field (DateTime.Now.TimeOfDay.ToString())
        ret <- getForDate_market_data numRecords date field tickers
                    |> Map.fold ( fun s f l -> Map.add f l s ) ret

    ret




let getStaticData tickers fields =

     [

        for field in fields ->

            get_static_data field tickers

     ]

     |> List.reduce ( fun m1 m2 -> (  m1 , m2 ) ||> Map.fold ( fun sm k v -> Map.add k v sm ) )


     

let getForexData forexIndicator baseCurrency currencies =

    let currencyPairs = formCurrencyPairs forexIndicator baseCurrency currencies

    get_market_data DatabaseFields.price currencyPairs |> Map.find DatabaseFields.price 




let getConversionData ( fxConversionRates : YieldConversionRates ) =

    let ccy2Rate , rateNames = fxConversionRates.conversionRate |> Map.ofList ,  fxConversionRates.conversionRate |> List.unzip |> snd |> set |> Set.toList
    
    let rate2Data =

        get_market_data DatabaseFields.yld rateNames 
        
            |> Map.find DatabaseFields.yld

    ccy2Rate

        |> Map.map ( 
        
            fun _ r -> 
                
                if rate2Data.ContainsKey r then

                    rate2Data.Item r

                else

                    printfn "no data was found for %s" r

                    List.empty

                )
     


let getIndexMembersData baseCurrency forexIndicator forexData name2Currency indexConstraints date marketData indexes  =

    let dateInt = date |> obj2Date
    
    let indexMembersData = get_index_members indexes date
    printfn "done members %s" (DateTime.Now.TimeOfDay.ToString())            
    let name2FxRate = name2ForexRate forexIndicator baseCurrency forexData name2Currency dateInt
    printfn "now weights %s" (DateTime.Now.TimeOfDay.ToString())
    let knownIndexWeights = indexes |> getWeights_index_members false date 
    let indexesWithNoWeights = indexes |> Set.filter ( fun i -> Map.containsKey i knownIndexWeights |> not )
    printfn "done weights %s" (DateTime.Now.TimeOfDay.ToString())
        
    memberWeights dateInt name2FxRate marketData indexConstraints.priceWeightedIndex indexConstraints.cappedIndex indexMembersData indexesWithNoWeights
                |> Map.fold ( fun s k l -> Map.add k l s ) knownIndexWeights



    
let equitiesPortfolioForDates ( input : EquitiesPortfolioInput ) ( fxConversionRates : YieldConversionRates ) dates =
 
    let featureTypes = input.featureTypes

    let conversionData = getConversionData fxConversionRates
    let allTickers = getUnion_equity_universe ()
    let allStaticData = getStaticData allTickers input.staticDataFields
    let forexData = allStaticData |> Map.find DatabaseFields.currency |> List.map ( fun ( _ , c ) -> c ) |> getForexData input.forexIndicator input.baseCurrency   

    let allGics = Map.find DatabaseFields.gicsIndustry allStaticData |> List.fold ( fun s ( _ , g ) -> g :: s ) List.empty
    
    let allMarketData = Map.empty |> ref
    let globalData = Map.empty |> ref
    let lastUpdate = DateTime.MaxValue |> ref
    let descendingDates = dates |> Seq.toArray |> Array.sort  |> Array.rev

    [

        for date in descendingDates ->
            
            if date >= ( !lastUpdate ).AddYears( - updateMktDataInterval ) then
                allMarketData := !allMarketData
                globalData := !globalData
            else
                lastUpdate := date
                allMarketData := getMarketData input.maxDataItems date allTickers input.marketDataFields
                globalData := getMarketData input.maxDataItems date allGics input.globalDataFields

            let tickers = date |> get_equity_universe
            let tickersSet = tickers |> set
            
            let marketData = 
                !allMarketData
                    |> Map.map ( fun _ m -> m |> Map.filter ( fun k _ -> Set.contains k tickersSet ) ) 
            let staticData = 
                allStaticData
                    |> Map.map ( fun _ l -> l |> List.filter ( fun ( k , _ ) -> Set.contains k tickersSet ) )                           

            model3By3Equities featureTypes input.forexIndicator input.baseCurrency ( date |> obj2Date ) forexData conversionData staticData marketData !globalData tickers

    ]




let equityIndexesPortfolioForDates  ( input : IndexesPortfolioInput ) ( fxConversionRates : YieldConversionRates ) ( indexConstraints : IndexWeightConstraint ) dates =

    let featureTypes = input.underlyingPortfolio.featureTypes

    let conversionData = getConversionData fxConversionRates

    let allIndexes = input.indexNames |> List.concat |> set

    let allIndexMembers = getUnion_index_members allIndexes
    let allIndexMembersAndIndexes = allIndexes |> Seq.append allIndexMembers |> Seq.toList
    let allStaticData = getStaticData allIndexMembersAndIndexes input.underlyingPortfolio.staticDataFields
    let forexData = allStaticData |> Map.find DatabaseFields.currency |> List.map ( fun ( _ , c ) -> c ) |> getForexData input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency

    let allGics = Map.find DatabaseFields.gicsIndustry allStaticData |> List.fold ( fun s ( _ , g ) -> g :: s ) List.empty
    let name2Currency = allStaticData |> Map.find DatabaseFields.currency |> Map.ofList

    let allMarketData = Map.empty |> ref
    let globalData = Map.empty |> ref
    let lastUpdate = DateTime.MaxValue |> ref
    let descendingDates = dates |> Seq.toArray |> Array.sort  |> Array.rev

    [

        for date in descendingDates ->

            printfn "now doing %s %s" (date.ToString()) (DateTime.Now.TimeOfDay.ToString())
            if date >= ( !lastUpdate ).AddYears( - updateMktDataInterval ) then
                allMarketData := !allMarketData
//                globalData := !globalData
            else
                lastUpdate := date
                allMarketData := getMarketData input.underlyingPortfolio.maxDataItems date allIndexMembersAndIndexes input.underlyingPortfolio.marketDataFields
//                globalData := getMarketData input.underlyingPortfolio.maxDataItems date allGics input.underlyingPortfolio.globalDataFields
            printfn "got data for %s %s" (date.ToString()) (DateTime.Now.TimeOfDay.ToString())

            let dateInt = ( date |> obj2Date)
            printfn "now members %s" (DateTime.Now.TimeOfDay.ToString())

            let indexMemberWeights =
                getIndexMembersData input.underlyingPortfolio.baseCurrency input.underlyingPortfolio.forexIndicator forexData name2Currency indexConstraints date !allMarketData allIndexes
                                
            let tickers = indexMemberWeights |> Map.toList |> List.unzip |> snd |> List.concat |> List.map ( fun ( t , _ ) -> t ) |> set |> Set.union allIndexes

            let marketData = !allMarketData |> Map.map ( fun _ m -> m |> Map.filter ( fun k _ -> Set.contains k tickers ) )

            let staticData = 
                allStaticData
                    |> Map.map ( fun _ l -> l |> List.filter ( fun ( k , _ ) -> Set.contains k tickers ) )

            ( 
            
                dateInt , 
            
                if input.model = IndexPortfolioModel.twoLevel then

                    model2LevelsIndexes featureTypes input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency dateInt forexData conversionData staticData marketData !globalData indexMemberWeights input.indexNames
            
                elif input.model = IndexPortfolioModel.clusters then

                    modelEuclideanClusterIndexes featureTypes input.clusterModelMininimumNamesPerCluster input.clusterModelNumberOfClusters input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency dateInt forexData conversionData staticData marketData indexMemberWeights input.indexNames
            
                elif input.model = IndexPortfolioModel.bestWorst then

                    modelBestWorst featureTypes input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency dateInt forexData conversionData staticData marketData indexMemberWeights input.indexNames
            
                elif input.model = IndexPortfolioModel.bestWorstCluster then

                    modelBWClusterIndexes featureTypes false input.clusterModelMininimumNamesPerCluster input.clusterModelNumberOfClusters input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency dateInt forexData conversionData staticData marketData indexMemberWeights input.indexNames

//                elif input.model = IndexPortfolioModel.lowCorreationCluster then
//
//                    let indexReturns = calculateReturns 
//                    modelLowCorrelationClusterIndexes featureTypes input.clusterModelMininimumNamesPerCluster input.clusterModelNumberOfClusters input.underlyingPortfolio.forexIndicator input.underlyingPortfolio.baseCurrency dateInt forexData conversionData staticData marketData indexMemberWeights indexReturns input.indexNames

                else

                    raise( Exception ( "unknown index portfolio model " + input.model.ToString() ) )
            )

    ]




let equitiesPortfolio ( input : EquitiesPortfolioInput ) featureTypes ( fxConversionRates : YieldConversionRates ) = 

    [ DateTime.Today ]

        |> equitiesPortfolioForDates input fxConversionRates

        |> List.map ( fun ( l , s ) -> printfn "final long: \n%s" (paste l) ; printfn "final short: \n%s" ( paste s ) )




let equitiesPortfolioHistorical ( input : EquitiesPortfolioHistoricalInput ) featureTypes ( fxConversionRates : YieldConversionRates ) = 

    datesAtFrequency input.frequency input.startDate input.endDate

        |> Array.map ( fun d -> date2Obj d )

        |> equitiesPortfolioForDates input.equitiesPortfolioInput fxConversionRates

        |> List.map ( fun ( l , s ) -> printfn "final long: \n%s" (paste l) ; printfn "final short: \n%s" ( paste s ) )




let equityIndexesPortfolio ( input : IndexesPortfolioInput )  ( fxConversionRates : YieldConversionRates ) ( indexConstraints : IndexWeightConstraint ) =

    [ DateTime.Today ]

        |> equityIndexesPortfolioForDates input fxConversionRates indexConstraints 

        |> List.iter ( 
        
                        fun ( d , a ) -> 

                            a
                                                    
                                |> Array.iter ( fun  ( lIndexes , sIndexes ) ->
                                                    printfn "\n\n%d\n" d
                                                    printfn "long: \n%s\n\n" ( paste lIndexes  )
                                                    printfn "short: \n%s" ( paste sIndexes )
                                                )

                    )





let testOutput ( input : IndexesPortfolioHistoricalInput ) dates longShorts =

    let dates = dates |> Array.rev
    let allNames = input.indexesPortfolioInput.indexNames
    let developedNames = allNames.[ 0 ] |> set
    let developingNames = if allNames.Length > 1 then allNames.[ 1 ] |> set else Set.empty
    let allIndexes = allNames |> List.concat
    let clusterDevelopedPortfolios = Array.init input.indexesPortfolioInput.clusterModelNumberOfClusters ( fun _ -> Array2D.create dates.Length developedNames.Count 0. ) 
    let clusterDevelopingPortfolios = Array.init input.indexesPortfolioInput.clusterModelNumberOfClusters ( fun _ -> Array2D.create dates.Length developingNames.Count 0. )
    let updateClusterPortfolios a = 
        a |> Array.map ( fun ( ml , ms ) -> marketNeutral ( ml |> keySet , ms |> keySet ) )

    let partitionByNames names a =
        a   |> Array.map ( fun ( ml , ms ) ->                        
                            if Set.intersect ( ml |> keySet ) names <> Set.empty || Set.intersect ( ms |> keySet ) names <> Set.empty  then
                                    Some ( ml , ms )
                            else None 
                        )
                                            
            |> Array.filter ( fun m  -> m.IsSome ) 
            |> Array.map ( fun m  -> m.Value  )
                                    
    let writeHeader names ( sw : IO.StreamWriter ) =
        sw.Write( "Date" )
        for name in names do
            sw.Write ( "," + name )
        sw.WriteLine()

    let fillWith0s date nameCount ( sw : IO.StreamWriter ) =
        sw.Write( date.ToString() )
        for i in 0 .. nameCount-1 do
            sw.Write ( ",0.0" )
        sw.WriteLine( )

        

    let portfolios =        
        longShorts |> List.map ( fun ( d , a ) -> ( d , a |> updateClusterPortfolios ) ) 
    
    let developedLongShorts = 
        longShorts |> List.map ( fun ( d , a ) ->(d, partitionByNames developedNames a)  )

    let developingLongShorts = 
        longShorts |> List.map ( fun ( d , a ) -> (d, partitionByNames developingNames a ) )

    let swDeveloped = 
        [
        for i in 0 ..  input.indexesPortfolioInput.clusterModelNumberOfClusters-1 ->
            new IO.StreamWriter( "../output/developedPortfolio"+i.ToString()+".csv" )
        ]
    for sw in swDeveloped do
        writeHeader developedNames sw

    for i in 0 .. dates.Length-1 do
        let date = dates.[ i ] |> obj2Date
        let portDate , clustersNames = developedLongShorts.[ i ] |> ( fun p -> ( fst p , snd p ) )
        let clusters = updateClusterPortfolios clustersNames
        if portDate <> date then
            raise( Exception ( "something off " + date.ToString() + "," + portDate.ToString() ) )
        for j in 0 .. swDeveloped.Length-1 do
            if j < clusters.Length then 
                let cluster = clusters.[ j ]
                swDeveloped.[j].Write( date.ToString() )
                for name in developedNames do
                    if cluster.ContainsKey name then
                        swDeveloped.[j].Write ( "," + (cluster.Item name).ToString() )
                    else
                        swDeveloped.[j].Write ( ",0.0" )
                swDeveloped.[j].WriteLine ( )
            else
                fillWith0s date developedNames.Count swDeveloped.[j]


    let swDeveloping = 
        [
        for i in 0 ..  input.indexesPortfolioInput.clusterModelNumberOfClusters-1 ->
            new IO.StreamWriter( "../output/developingPortfolio"+i.ToString()+".csv" )
        ]
    for sw in swDeveloping do
        writeHeader developingNames sw

    for i in 0 .. dates.Length-1 do
        let date = dates.[ i ] |> obj2Date
        let portDate , clustersNames = developingLongShorts.[ i ] |> ( fun p -> ( fst p , snd p ) )
        let clusters = updateClusterPortfolios clustersNames
        if portDate <> date then
            raise( Exception ( "something off " + date.ToString() + "," + portDate.ToString() ) )
        for j in 0 .. swDeveloping.Length-1 do
            if j < clusters.Length then
                let cluster = clusters.[ j ]
                swDeveloping.[j].Write( date.ToString() )
                for name in developingNames do
                    if cluster.ContainsKey name then
                        swDeveloping.[j].Write ( "," + (cluster.Item name).ToString() )
                    else
                        swDeveloping.[j].Write ( ",0.0" )
                swDeveloping.[j].WriteLine ( )
            else
                fillWith0s date developingNames.Count swDeveloping.[j]


    for i in 0 ..  input.indexesPortfolioInput.clusterModelNumberOfClusters-1 do
        swDeveloped.[i].Close()
        swDeveloping.[i].Close()


    let mergeClusters clusters =
        clusters
            |> Array.fold ( fun ( sl , ss ) ( ml , ms ) -> 
                                if Map.isEmpty ml || Map.isEmpty ms then
                                    ( sl , ss )  
                                else
                                    ( ml |> keySet |> Set.union sl , ms |> keySet |> Set.union ss )
                                ) ( Set.empty , Set.empty )

    let swPortfolioFile = new IO.StreamWriter( "../output/allIndexesPortfolio.csv" )
    writeHeader allIndexes swPortfolioFile
    for i in 0 .. dates.Length-1 do
        let date = dates.[ i ] |> obj2Date
        let portDate , clusters = longShorts.[ i ] |> ( fun p -> ( fst p , snd p ) )
        let portfolio = mergeClusters clusters |> marketNeutral
        if portDate <> date then
            raise( Exception ( "something off " + date.ToString() + "," + portDate.ToString() ) )
        swPortfolioFile.Write( date.ToString() )
        for name in allIndexes do
            if portfolio.ContainsKey name then
                swPortfolioFile.Write ( "," + (portfolio.Item name).ToString() )
            else
                swPortfolioFile.Write ( ",0.0" )
        swPortfolioFile.WriteLine ( )
    swPortfolioFile.Close()

    let swDevelopedFile = new IO.StreamWriter( "../output/developedPortfolio.csv" )
    writeHeader developedNames swDevelopedFile
    for i in 0 .. dates.Length-1 do
        let date = dates.[ i ] |> obj2Date
        let portDate , clusters = developedLongShorts.[ i ] |> ( fun p -> ( fst p , snd p ) )
        let portfolio = mergeClusters clusters |> marketNeutral
        if portDate <> date then
            raise( Exception ( "something off " + date.ToString() + "," + portDate.ToString() ) )
        swDevelopedFile.Write( date.ToString() )
        for name in developedNames do
            if portfolio.ContainsKey name then
                swDevelopedFile.Write ( "," + (portfolio.Item name).ToString() )
            else
                swDevelopedFile.Write ( ",0.0" )
        swDevelopedFile.WriteLine ( )
    swDevelopedFile.Close()

    let swDevelopingFile = new IO.StreamWriter( "../output/developingPortfolio.csv" )  
    writeHeader developingNames swDevelopingFile
    for i in 0 .. dates.Length-1 do
        let date = dates.[ i ] |> obj2Date
        let portDate , clusters = developingLongShorts.[ i ] |> ( fun p -> ( fst p , snd p ) )
        let portfolio = mergeClusters clusters |> marketNeutral
        if portDate <> date then
            raise( Exception ( "something off " + date.ToString() + "," + portDate.ToString() ) )
        swDevelopingFile.Write( date.ToString() )
        for name in developingNames do
            if portfolio.ContainsKey name then
                swDevelopingFile.Write ( "," + (portfolio.Item name).ToString() )
            else
                swDevelopingFile.Write ( ",0.0" )
        swDevelopingFile.WriteLine ( )
    swDevelopingFile.Close()





let equityIndexesPortfolioHistorical ( input : IndexesPortfolioHistoricalInput )  ( fxConversionRates : YieldConversionRates ) ( indexConstraints : IndexWeightConstraint ) =
    
    let dates =

        datesAtFrequency input.frequency input.startDate input.endDate

            |> Array.map ( fun d -> date2Obj d )

    let longShorts =

        dates |> equityIndexesPortfolioForDates input.indexesPortfolioInput fxConversionRates indexConstraints
    let junkSw = new IO.StreamWriter "../output/longShort.csv"
    longShorts

        |> List.iter ( 
        
                        fun ( d , a ) -> 

                            a
                                                    
                                |> Array.iter ( fun  ( lIndexes , sIndexes ) ->
                                                    let mutable txt = d.ToString() 
                                                    txt <- txt + ( lIndexes |> Map.toList |> List.fold ( fun t ( i , s ) -> t + "," + i + ","+ s.ToString() ) txt ) + ",,"
                                                    txt <- txt  + ( sIndexes |> Map.toList |> List.fold ( fun t ( i , s ) -> t + "," + i + ","+ s.ToString() ) "" )
                                                    junkSw.WriteLine ( txt )
                                                )

                    )


    junkSw.Close()

    testOutput input dates longShorts

    ()

