module SA2.Options.OptionWeights


open SA2.Common.Math


let averageAreaMethod ( strikes : float [] ) =
     
    let nStrikes = strikes.Length
    let sortedStrikes = strikes |> Array.sort

    // Note: in Array.mapi() remember we took the first and last strikes out; so i in the resulting array is i + 1 in the original strikes array
    let ret = sortedStrikes.[ 1 .. nStrikes - 2 ] |> Array.mapi ( fun i k -> ( k , ( sortedStrikes.[ i + 2 ] - sortedStrikes.[ i ] ) / 2.0  ) ) |> Map.ofArray

    // add the 2 end strikes and return
    ret |> Map.add sortedStrikes.[0] ( sortedStrikes.[1] - sortedStrikes.[0] ) |> Map.add sortedStrikes.[ nStrikes - 1 ] ( sortedStrikes.[ nStrikes - 1 ] - sortedStrikes.[ nStrikes - 2 ] )




let averageStepMethod ( strikes : float [] ) =
 
    let nStrikes = strikes.Length
    let sortedStrikes = strikes |> Array.sort

    sortedStrikes.[ 1 .. nStrikes - 1 ] .-. sortedStrikes.[ 0 .. nStrikes - 2 ]  |> Array.average