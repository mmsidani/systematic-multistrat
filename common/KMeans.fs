module SA2.Common.KMeans


open System
open MathNet.Numerics

open Math




let private maxIterationsConstant = 101




let euclideanDistance v centroid =

    ( centroid , v ) ||> Array.map2 ( fun ci vi -> ( ci - vi ) ** 2.0 ) |> Array.sum |> sqrt 




let correlationDistance v centroid =

    // "distance" with a wink 

    ( centroid , v ) ||> correlation




let private classify useMin ( distance : float[] -> float[] -> float ) ( centroids : float [][] ) ( vs : ( string * float [] ) seq ) = 

    if useMin then

        vs  |> Seq.groupBy ( fun ( _ , v ) -> centroids |> Array.minBy ( distance v ) )

    else

        vs  |> Seq.groupBy ( fun ( _ , v ) -> centroids |> Array.maxBy ( distance v ) )




let computeCentroid ( vs : float[] seq ) =

    let num = Seq.length vs |> float 

    vs |> Seq.reduce ( fun v0 v1 -> ( v0 , v1 ) ||> Array.map2 ( fun v0i v1i -> v0i + v1i ) |> Array.map ( fun vi -> vi / num ) )




let private initForgyCentroids numClusters namedArrays =

    namedArrays |> Map.toArray |> Array.unzip |> snd |> ( fun a -> a.[ 0 .. numClusters - 1 ] )




let private updateCentroids useMin ( distance : float[] -> float[] -> float ) ( vs : ( string * float [] ) seq ) centroids = 
    
    classify useMin distance centroids vs 
    
        |> Seq.map 
        
            ( 
        
            fun ( _ , s ) ->

                    let newCentroid = s |> Seq.map ( fun ( _ , w ) -> w ) |> computeCentroid

                    ( newCentroid , s |> Seq.map ( fun ( i , _ ) -> i ) |> set )

            )




let rec computeClusters useMin minPerCluster numClusters ( distance : float[] -> float[] -> float ) ( namedArrays : Map< string , float[] > )  =

    if namedArrays.Count < numClusters then

        raise ( Exception ( "more clusters than elements") )

    let updateCentroidsVs = namedArrays |> Map.toArray |> updateCentroids useMin distance

    let mutable centroids = initForgyCentroids numClusters namedArrays
    let mutable clusters = Array.create numClusters Set.empty
    let mutable notConverged = true
    let mutable numIterations = 0
    let mutable name2Cluster = Map.empty

    while notConverged && numIterations < maxIterationsConstant do
        
        let newCentroids , newClusters = updateCentroidsVs centroids |> Seq.toArray |> Array.unzip
        let newName2Cluster = 
            newClusters |> Array.mapi ( fun i s -> s |> Set.map ( fun n -> ( n , i ) ) |> Set.toArray ) |> Array.concat |> Map.ofArray
        
        notConverged <- clusters.Length = newClusters.Length
                        && (  clusters , newClusters ) ||> Array.forall2 ( fun s0 s1 -> s0.Equals s1  ) |> not

        numIterations <- numIterations + 1
        clusters <- newClusters
        centroids <- newCentroids     
        name2Cluster <- newName2Cluster 
        
    if minPerCluster > 0 then

        if  clusters |> Array.forall ( fun s -> s.Count >= minPerCluster ) |> not then

            if numClusters - 1 > 2 then 

                computeClusters useMin minPerCluster ( numClusters - 1 ) distance namedArrays

            else
            clusters
        else
        clusters
    else
    clusters
