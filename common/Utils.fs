module SA2.Common.Utils

open System
open Microsoft.SolverFoundation.Common

exception VarianceException of string
exception KeysNotFound of string Set




let concatFileInfo line ( x : List<'a> )  = List.fold (fun ret y -> ret + " " + y.ToString() ) ( "Error in file " + __SOURCE_DIRECTORY__ + "\\" + __SOURCE_FILE__ + ", line " + line + ": ") x




let float2rational x = Rational.op_Implicit (x : float)




let paste v =

    Seq.fold 
    
        ( 
    
        fun s vi -> 
    
            s + vi.ToString() +  "\n"
                    
        ) "" v




let keySet m =

    if Map.isEmpty m  then

        Set.empty

    else

        m |> Map.fold ( fun s k _ -> Set.add k s ) Set.empty




let valueSet m =

    if Map.isEmpty m then

        Set.empty

    else

        m |> Map.fold ( fun s _ v -> Set.add v s ) Set.empty




let value2Keys m =

    let resultsInit : Map< 'a , 'b list > = m |> valueSet |> Set.toList |> List.map ( fun v -> ( v , List.empty ) ) |> Map.ofList

    m
        
        |> Map.fold ( fun s k v -> Map.add v ( k :: ( Map.find v s ) ) s ) resultsInit




let buildMap2List l =

    let names = 

        l
            |> Seq.map ( fun ( n , _ ) -> n )
            |> set
            |> Set.toList

    let emptyMap = 
    
        ( names , List.init ( List.length names ) ( fun _ -> Set.empty ) )

            ||> List.zip

            |> Map.ofList

    Seq.fold ( fun s ( k , v ) -> Map.add k (  Map.find k s |> Set.add v ) s ) emptyMap l 

        |> Map.map ( fun _ s -> Set.toList s )




let isLongerSeq length sq =

    // testing length of sequence without triggering full computation

    if Seq.isEmpty sq && length <> 0 then

        false 

    elif length = 0 then

        true

    else

        Seq.windowed length sq |> Seq.isEmpty |> not




let alignOnSortedDates data =

    // decreasing order of dates

    let datesList = 

        data

            |> Map.toList
            |> ( List.unzip >> snd )
            |> List.map ( fun l -> l |> List.map ( fun ( d , _ ) -> d ) |> Set.ofList )


    let commonDates = Set.intersectMany datesList

    data |> Map.map ( fun _ l -> l |> List.filter ( fun ( d , _ ) -> Set.contains d commonDates  ) |> List.sort |> List.rev )




let fillGaps extrapolate dates data =

    let sortedData = data |> List.sortBy ( fun ( d , _ ) -> d )

    let dataDatesPairs = List.map ( fun ( d , _ ) -> d ) sortedData |> Seq.pairwise |> Seq.toArray

    let sortedDates = dates |> Seq.filter ( fun d -> dataDatesPairs.[ 0 ] |> fst <= d ) |> Seq.sort |> Array.ofSeq

    if sortedDates.Length = 0 then raise ( Exception "can't fill values from the future")

    let datesWithinBounds = sortedDates |> Array.filter ( fun d -> dataDatesPairs.[ dataDatesPairs.Length - 1 ] |> snd >= d )

    let datesBeyond = 
        if datesWithinBounds.Length < sortedDates.Length then
            sortedDates.[ datesWithinBounds.Length .. ]
        else
            Array.empty
    

    let mapped = data |> Map.ofList

    let upperDate = ref -1
    let lastValue = ref 0.0 
    
    [
        for d in datesWithinBounds ->

            if d > !upperDate then

                let pairIndex = Array.findIndex ( fun ( d0 , d1 ) -> d0 <= d && d < d1 ) dataDatesPairs
                upperDate := dataDatesPairs.[ pairIndex ] |> snd 
                lastValue := Map.find ( dataDatesPairs.[ pairIndex ] |> fst ) mapped

                ( d , !lastValue )

            elif d = !upperDate then

                ( d ,  Map.find !upperDate mapped )

            else

                ( d , !lastValue )
    ]

    |> List.append ( 
    
        if extrapolate then
            let value = sortedData.[ sortedData.Length - 1 ] |> snd
            List.init datesBeyond.Length ( fun i -> ( datesBeyond.[ i ] ,  value ) ) // empty if datesBeyond.Length = 0
        else
            List.empty
        )
