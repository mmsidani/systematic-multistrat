module SA2.Equity.BottomUpModels


open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.Math
open SA2.Common.Dates
open SA2.Common.KMeans
open SA2.Common.Returns
open SA2.Common.ForEx



open StaticPartitions
open FeaturesNames
open FeaturesIndustries
open EquityMetrics
open IndexWeights



// MissingDataAlert: index scores do not include scores for names that because of some missing data were excluded at some point 



let private quantileProbsConstant = [| 0.2 ; 0.4 ; 0.6 ; 0.8 ; 1.0 |] |> Array.sort // to stress the point

let private growthPeriodConstant = 12





let private mapName2Currency staticData =

    if Map.containsKey DatabaseFields.currency staticData then
        
        Map.find DatabaseFields.currency staticData |> Map.ofList

    else

        Map.empty




let private mapName2Gics staticData =

    if Map.containsKey DatabaseFields.gicsIndustry staticData then

        Map.find DatabaseFields.gicsIndustry staticData |> Map.ofList

    else

        Map.empty




let private getAllGics index2GicsAndWeights = 

    index2GicsAndWeights |> Map.fold ( fun s _ m -> m |> keySet |> Set.union s ) Set.empty |> Set.toArray




let private mapIndex2GicsWeights allGics index2GicsAndWeights = 

    index2GicsAndWeights

        |> Map.map ( fun _ m -> allGics |> Array.map ( fun g -> if Map.containsKey g m then Map.find g m else 0.0 ) )




let private mapIndex2SectorWeights allGics index2GicsAndWeights =

    let gics2Sector = 
    
        allGics

            |> Array.map ( fun ( g : string ) -> ( g , g.Substring( 0 , 2 ) ) )

            |> Map.ofArray

    let allSectors = gics2Sector |> valueSet |> Set.toArray
        
    let index2Sector2Weight = 

        mapIndex2GicsWeights allGics index2GicsAndWeights

            |> Map.map ( fun _ a -> 

                            a 

                                |> Array.mapi ( fun i w -> ( w , Map.find allGics.[ i ] gics2Sector ) ) 
                                |> Seq.groupBy ( fun ( _ , s ) -> s ) 
                                |> Seq.map ( fun ( s , x ) -> ( s , x |> Seq.map ( fun ( w , _ ) -> w ) |> Seq.sum ) )
                                |> Map.ofSeq
                        )

    index2Sector2Weight
    
        |> Map.map 
            ( fun _ m -> 
                    allSectors 
                        |>  Array.map ( fun s -> 
                                            if Map.containsKey s m then
                                                Map.find s m
                                            else
                                                0.0
                                        )
            )
                        


    
let private computeGicsWeights nameToGics indexNamesWeights =
    
        indexNamesWeights

            |> Map.map ( fun _ l -> l |> List.map ( fun ( n , w ) -> ( Map.find n nameToGics , w ) ) )

            |> Map.map ( 
            
                        fun _ l ->

                            let iGics = l |> List.map ( fun ( g , _ ) -> g ) |> set

                            let gics2WeightInit = iGics |> Set.fold ( fun m g -> Map.add g 0.0 m ) Map.empty

                            l |> List.fold ( fun s ( g , w ) -> Map.add g ( Map.find g s + w ) s ) gics2WeightInit

                        )




let namesScores featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexMembersData indexes =

    let name2Currency = mapName2Currency staticData 
              
    let name2Gics = mapName2Gics staticData

    let instruments = 

        indexes

            |> List.fold ( fun s i -> ( Map.find i indexMembersData |> set , s ) ||> Set.union ) Set.empty

    let priorDate = ( date |> date2Obj ).AddMonths( - growthPeriodConstant ) |> obj2Date

    let combiner = averageZScore /// averageQuantilesPerBucket quantileProbsConstant name2Gics /// 
    let ranker = rankByQuantiles quantileProbsConstant

    combineFeatures featureTypes combiner ranker forexIndicator baseCurrency date priorDate staticData marketData forexData conversionData name2Gics name2Currency instruments

        |> Map.fold ( 

                    // so here we drop the rank that was assigned by combineFeatures

                    fun ( ln , ls ) _ l -> l |> List.unzip ||> ( fun ns ss -> List.append ns ln , List.append ss ls )

                    ) 
                        
                    ( List.empty , List.empty )

        ||> List.zip

        |> Map.ofList




let indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights indexes = 
    let junkSw = new System.IO.StreamWriter ( "../output/percentageScore.csv" , true )

    let indexMembersData = 
    
        indexNamesWeights

            |> Map.map ( fun _ l -> l |> List.map ( fun ( n , _ ) -> n ) )

    let names2Scores = namesScores featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexMembersData indexes

    let indexesSet = indexes |> set

    indexNamesWeights

        |> Map.filter ( fun i _ -> indexesSet.Contains i )

        |> Map.map ( fun i l -> 
        
                        l 
                        
                        |> Seq.map ( fun ( n , w ) -> 
                        
                                        if Map.containsKey n names2Scores then
                                            
                                            ( w , w * Map.find n names2Scores )
                                        
                                        else
                                            
                                            ( 0.0 , 0.0 ) )
                                        
                        |> ( fun l -> l |> Seq.toList |> List.unzip |> ( fun ( ws , ss ) -> ( List.sum ws , List.sum ss ) ) 
                        
                                                                    |> ( fun ( wSum , scoreSum ) -> junkSw.WriteLine( date.ToString() + "," + i + ","+wSum.ToString()+","+scoreSum.ToString() ) ; (wSum,scoreSum)) 

                                                                    |> ( fun ( _ , scoreSum ) -> scoreSum ) ) )

        |> ( fun ret -> junkSw.Close() ; ret )




let indexScoreFromIndustry forexIndicator baseCurrency date forexData conversionData staticData marketData globalData indexNamesWeights indexes = 

    let nameToCurrency = mapName2Currency staticData 
              
    let nameToGics = mapName2Gics staticData
    
    let instruments = indexNamesWeights |> Map.toList |> List.unzip |> snd |> List.map ( fun l -> l |> List.map ( fun ( n , _ ) -> n ) ) |> List.concat |> set |> Set.toList
    
    let priorDate = ( date |> date2Obj ).AddMonths( - growthPeriodConstant ) |> obj2Date
    
    let gicsRanker x = x |> Map.map ( fun k v -> v ) // I want the average z-score to be returned so pass identity map
    
    let gics2Score = 
    
        sizeFragGrowth averageZScore gicsRanker forexIndicator baseCurrency date priorDate staticData marketData forexData globalData nameToGics nameToCurrency instruments

                |> Map.toList

                |> List.fold ( fun s ( score , l ) -> // Note: we're indexing by z-score on output from sizeFragGrowth. chances are every one of those l's has a single gics in them
                        
                                    l |> List.fold ( fun ss n -> Map.add n score ss )  s  

                             ) Map.empty
    
    let gicsWeights = computeGicsWeights nameToGics indexNamesWeights

    let indexesSet = indexes |> set

    gicsWeights

        |> Map.filter ( fun i _ -> indexesSet.Contains i )

        |> Map.map ( fun i m -> m |> Map.filter ( fun g _ -> gics2Score.ContainsKey g ) |> Map.fold ( fun s g w -> s + w * Map.find g gics2Score ) 0.0 )




let model2LevelsIndexes featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData globalData indexMemberWeights indexClusters = 

    let indexes = List.concat indexClusters // not using clusters here but had to make interface identical to the euclidean clustering model

    let scoreFromIndustriesToIndex = 
    
        indexScoreFromIndustry forexIndicator baseCurrency date forexData conversionData staticData marketData globalData indexMemberWeights indexes

            |> rankByQuantiles quantileProbsConstant

            |> value2Keys

    let longIndexes =

        let floatLength = float quantileProbsConstant.Length

        if Map.containsKey floatLength scoreFromIndustriesToIndex then
        
            Map.find floatLength scoreFromIndustriesToIndex

                |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexMemberWeights

        else

            Map.empty

    let shortIndexes =

        if Map.containsKey 1.0 scoreFromIndustriesToIndex then
        
            Map.find 1.0 scoreFromIndustriesToIndex

                |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexMemberWeights

        else

            Map.empty

    [| ( longIndexes , shortIndexes ) |]




let modelEuclideanClusterIndexes featureTypes minPerCluster numClusters forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights indexClusters = 

    // split each cluster into longs and shorts

    let name2Currency = mapName2Currency staticData 
              
    let name2Gics = mapName2Gics staticData
    let index2Gics2Weight = computeGicsWeights name2Gics indexNamesWeights

    let allGics = getAllGics index2Gics2Weight
    let index2Weights = mapIndex2SectorWeights allGics index2Gics2Weight /// mapIndex2GicsWeights allGics index2Gics2Weight /// 

    let clusteredIndexes = 
    
        indexClusters // for instance, developed / developing

            |> List.filter ( fun l -> List.isEmpty l |> not )

            |> List.map ( fun l -> set l )
        
            |> List.map ( fun s ->

                            index2Weights

                                |> Map.filter ( fun i _ -> Set.contains i s )
                                
                                |> computeClusters true minPerCluster numClusters euclideanDistance

                        )

    let indexScores = indexClusters |> List.concat |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights

    clusteredIndexes 

        |> List.map ( fun aa ->
                        
                        aa

                            |> Array.map 
        
                                ( fun s -> 

                                    s 
                                        |> Set.map ( fun i -> ( i , Map.find i indexScores ) ) 
                                        |> Set.toArray 
                                        |> Array.sortBy ( fun ( _ , v ) -> v ) 
                                        |> ( fun ab -> 
                                                let num = ab.Length / 3
                                                if num >= 1 then
                                                    [| ( ab.[ ab.Length - num .. ab.Length - 1 ] |> Map.ofArray , ab.[ 0 .. num-1 ]  |> Map.ofArray ) |]
                                                elif ab.Length = 2 then
                                                    [| ( [| ab.[ ab.Length - 1 ] |]  |> Map.ofArray , [| ab.[ 0 ] |]  |> Map.ofArray ) |]
                                                else // ab.Length = 1
                                                    [| ( [| ab.[ 0 ] |] |> Map.ofArray , Map.empty ) |]
                                            )

                                )
                    )

        |> Array.concat

        |> Array.concat




let modelBWClusterIndexes featureTypes doMerge minPerCluster numClusters forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights indexClusters = 

    let name2Currency = mapName2Currency staticData 
              
    let name2Gics = mapName2Gics staticData
    let index2Gics2Weight = computeGicsWeights name2Gics indexNamesWeights

    let name2FxRate = name2ForexRate forexIndicator baseCurrency forexData name2Currency date

    let allGics = getAllGics index2Gics2Weight
    let index2Weights = mapIndex2SectorWeights allGics index2Gics2Weight /// mapIndex2GicsWeights allGics index2Gics2Weight /// 

    let indexMarketCaps = 
        getDbDataForDate marketData date DatabaseFields.marketCap
            |> Map.map ( fun i v -> Map.find i name2FxRate * v )

    let clusteredIndexes = 
    
        indexClusters // for instance, developed / developing

            |> List.filter ( fun l -> List.isEmpty l |> not )

            |> List.map ( fun l -> l |> List.filter ( fun i -> Map.containsKey i indexMarketCaps ) )
            
            |> ( fun cs -> 

                    if doMerge then // for instance, merge developed and developing and treat them as one big universe

                        cs |> List.reduce ( fun s0 s1 -> List.append s0 s1 ) |> ( fun s -> [ s ] )
                    else
                    
                        cs

                )

            |> List.map ( fun l -> set l )
        
            |> List.map ( fun s ->

                            index2Weights

                                |> Map.filter ( fun i _ -> Set.contains i s )
                                
                                |> computeClusters true minPerCluster numClusters euclideanDistance

                        )

            |> List.toArray
            
    

    let clusterMarketCaps =
        clusteredIndexes
            |> Array.map ( fun a -> a |> Array.map ( fun s -> s |> Set.map ( fun i -> Map.find i indexMarketCaps ) |> Seq.sum ) )

    let indexScores = 
        clusteredIndexes 
            |> Array.map ( fun a -> a |> Set.unionMany |> Set.toList |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights )
            |> Array.reduce ( fun m0 m1 -> Map.fold ( fun s k v -> Map.add k v s ) m0 m1 )

    let clusterScores = 
        clusteredIndexes 
            |> Array.mapi ( fun i a -> a |> Array.mapi ( fun j s -> s |> Set.map ( fun ind -> Map.find ind indexScores * Map.find ind indexMarketCaps / clusterMarketCaps.[ i ].[ j ] ) |> Seq.sum ))

    ( clusteredIndexes , clusterScores )

        ||> Array.zip

        |> Array.map ( fun ( sa ,  sf ) -> ( sa , sf ) ||> Array.zip )

        |> Array.map ( fun a -> a |> Array.sortBy ( fun ( _ , f ) -> f ) )

        |> Array.map ( fun a ->  

                        if a.Length > 1 then

                            [| a.[ a.Length - 1 ] |> ( fun ( s , _ ) -> s |> Set.map ( fun i -> ( i , Map.find i indexScores ) ) ) |> Map.ofSeq , 

                                a.[ 0 ] |> ( fun ( s , _ ) -> s |> Set.map ( fun i -> ( i , Map.find i indexScores ) ) ) |> Map.ofSeq |] 

                        else // a.Length = 1

                            [| ( Map.empty , Map.empty ) |] )
                        

        |> Array.concat




let modelLowCorrelationClusterIndexes featureTypes minPerCluster numClusters forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights indexReturns indexClusters = 

    let name2Currency = mapName2Currency staticData 
    
    let name2FxRate = name2ForexRate forexIndicator baseCurrency forexData name2Currency date
    let indexMarketCaps = 
        getDbDataForDate marketData date DatabaseFields.marketCap
            |> Map.map ( fun i v -> Map.find i name2FxRate * v )

    let clusteredIndexes = 
    
        indexClusters // for instance, developed / developing

            |> List.filter ( fun l -> List.isEmpty l |> not )

            |> List.map ( fun l -> l |> List.filter ( fun i -> Map.containsKey i indexMarketCaps ) )
            
            |> ( fun cs -> cs |> List.reduce ( fun s0 s1 -> List.append s0 s1 ) )

            |> set
        
            |> ( fun s ->

                    indexReturns

                        |> Map.filter ( fun i _ -> Set.contains i s )
                                
                        |> computeClusters false minPerCluster numClusters correlationDistance

                )


    let indexScores = 
        clusteredIndexes 
            |> Array.map ( fun a -> a |> Set.toList |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights )
            |> Array.reduce ( fun m0 m1 -> Map.fold ( fun s k v -> Map.add k v s ) m0 m1 )

    let clusterScores = 
        clusteredIndexes 
            |> Array.map ( fun a -> a |> ( fun s -> s |> Set.map ( fun i -> Map.find i indexScores * Map.find i indexMarketCaps ) |> Seq.sum ) )

    let centroids =     
        clusteredIndexes
            |> Array.map( fun a -> a |> ( fun s -> ( s |> Set.map ( fun n -> Map.find n indexReturns ) |> computeCentroid ) ) )

    let iC , jC , _ =
        centroids 
            |> Array.mapi ( fun i c -> centroids |> Array.mapi ( fun j c1 -> ( j , correlation c c1 ) ) |> Array.minBy ( fun ( _ , cr ) -> cr ) |> ( fun ( j , cr ) -> ( i , j , cr ) ) )
            |> Array.minBy ( fun ( _ , _ , cr ) -> cr )

    let iScore , jScore = clusterScores.[ iC ] , clusterScores.[ jC ]
    let iIndexScores = clusteredIndexes.[ iC ] |> Set.map ( fun i -> ( i , Map.find i indexScores ) ) |> Map.ofSeq
    let jIndexScores = clusteredIndexes.[ jC ] |> Set.map ( fun i -> ( i , Map.find i indexScores ) ) |> Map.ofSeq

    if iScore > jScore then

        [| iIndexScores , jIndexScores |]

    else

        [| jIndexScores , iIndexScores  |]

                        
                        

let modelBestWorst featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights indexClusters =

    let indexScores = indexClusters |> List.concat |> indexScoreFromSingleNames featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData indexNamesWeights

    indexClusters 

        |> List.filter ( fun l -> l.IsEmpty |> not )

        |> List.map ( fun aa ->
                        
                        aa
                            |> List.toArray
                            |> Array.map ( fun i -> ( i , Map.find i indexScores ) )

                            |> Array.sortBy ( fun ( _ , v ) -> v ) 
                            |> ( fun ab -> 
                                    let num = ab.Length / 5
                                    if num >= 1 then
                                        [| ( ab.[ ab.Length - num .. ab.Length - 1 ] |> Map.ofArray , ab.[ 0 .. num-1 ]  |> Map.ofArray ) |]
                                    elif ab.Length > 1 then
                                        [| ( [| ab.[ ab.Length - 1 ] |]  |> Map.ofArray , [| ab.[ 0 ] |]  |> Map.ofArray ) |]
                                    else 
                                        [| ( Map.empty , Map.empty ) |]
                                )
                                

                    )

        |> Array.concat
