module SA2.Equity.RiskModels


open SA2.Common.Math



let minimumVariance longShort upperLongsBounds lowerShortsBounds rets =

    let longs , shorts = longShort |> ( fun ( l , s ) -> ( Seq.toArray l , Seq.toArray s ) )

    quadProg true longs shorts upperLongsBounds lowerShortsBounds rets




let minFactorExposure numPeriods longShort factorUniverse rets =

    let names = longShort ||> Seq.append |> set

    // Note: rets expected to have been filtered to have returns up to the date of interest

    let retsRevOrdered = 
    
        rets 

            |> Map.filter ( fun n _ -> Set.contains n names || Set.contains n factorUniverse )

            |> Map.map ( fun _ l -> l |> List.sortBy ( fun ( d , _ ) -> -d ) |> List.map ( fun ( _ , v ) -> v ) ) // Note: decreasing order
            
    let namesRets = Array.create names.Count Array.empty
    let namesList = names |> Set.toList
    let factorUnivRets = Array2D.create factorUniverse.Count numPeriods 0.0
    let factorUnivList = factorUniverse |> Set.toList

    for i in 0 .. namesList.Length - 1 do

        let l = Map.find namesList.[ i ] retsRevOrdered

        namesRets.[ i ] <-

            [|        

                for j in 0 .. numPeriods-1 ->

                    l.[ j ]
            |]
    
    for i in 0 .. factorUnivList.Length - 1 do

        let l = Map.find factorUnivList.[ i ] retsRevOrdered

        for j in 0 .. numPeriods-1 do

            factorUnivRets.[ i , j ] <- l.[ j ]

    let firstFactor =
    
        factorUnivRets
        
            |> firstPcaComp 

    let invStdevFactor = 1.0 / ( firstFactor |> variance |> sqrt )

    let exposures =

        namesList

            |> List.mapi ( fun i n -> ( n , ( namesRets.[ i ]  |> covArrays firstFactor ) / invStdevFactor ) )

            |> Map.ofList

    ()









let inverseVol longShort rets =

    let indexes = longShort ||> Seq.append

    let indexInverseVol = indexes |> Seq.map ( fun i -> ( i , 1.0 / ( Map.find i rets |> variance |> sqrt ) ) )

    let sumInverseVols = indexInverseVol |> Seq.sumBy ( fun ( _ , v ) -> v )

    let long , short = longShort |> ( fun ( l , s ) -> ( set l , set s ) )

    indexInverseVol

        |> Map.ofSeq 
        |> Map.map ( fun _ iv -> iv / sumInverseVols )
        |> Map.map ( fun i v -> 

                        if long.Contains i then

                            v

                        else // short must contain 

                            -v

                    )




let marketNeutral longShort  =

    // in the sense of matching weights between longs and shorts

    let portfolioWeightInLongs = 0.5 

    let longs , shorts = longShort

    let numLongs = Seq.length longs
    let numShorts = Seq.length shorts

    if numLongs = 0 || numShorts = 0 then

        ( longs , shorts )

            ||> Seq.append

            |> Seq.toArray

            |> Array.map ( fun i -> ( i , 0.0 ) )

    else

        let longPositions =

            longs

                |> Seq.map ( fun i -> ( i , portfolioWeightInLongs / ( float numLongs ) ) )

                |> Seq.toArray

        let shortPositions =

            shorts

                |> Seq.map ( fun i -> ( i , - ( 1.0 - portfolioWeightInLongs ) / ( float numShorts ) ) )

                |> Seq.toArray


        [| longPositions ; shortPositions |] |> Array.concat


    |> Map.ofArray

