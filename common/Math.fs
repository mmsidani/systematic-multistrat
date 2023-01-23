module SA2.Common.Math

open Microsoft.SolverFoundation.Common
open Microsoft.SolverFoundation.Solvers
open Microsoft.SolverFoundation.Services

open System

open MathNet.Numerics

open Utils

let private quantileDefinition = Statistics.QuantileDefinition.InverseCDF



let (.-.) (x : float[]) (y: float[]) =

    if x.Length <> y.Length then
        raise ( ["can't subtract 2 arrays of different lengths"] |> concatFileInfo __LINE__ |> VarianceException )

    Array.map2 (fun a b -> a - b ) x y




let rec bisec f x y tol =

    if f x * f y > 0.0 then

        raise( [ "Bad input" ] |> concatFileInfo __LINE__ |>  VarianceException   )

    let mid = ( x + y ) / 2.0 

    if  mid |> f |> Operators.abs < tol then

        mid

    else

        if f x * f mid < 0.0 then

            bisec f x mid tol

        else

            bisec f mid y tol
            



let nonLinearSolver objective initialGuess lowerBounds upperBounds = 

    let solution = NelderMeadSolver.Solve(objective, initialGuess, lowerBounds, upperBounds )
    
    ( solution.GetValue 1  , solution.GetValue 2 )




let variance ( x : float [] ) =

    Statistics.ArrayStatistics.Variance x




let standardDeviation ( x : float [] ) =

    Statistics.ArrayStatistics.StandardDeviation x 




let covariance ( x : float[ , ] ) =
    
    printfn "ACHTUNG ACHTUNG ACHTUNG: I don't trust this one"


    // observations of a variable are assumed to be in a row. why? typically we would have built x out of a float[][] using array2D and this last function populates by rows
    
    let rowsX , colsX = Array2D.length1 x , Array2D.length2 x

    let avgs = Array.init rowsX ( fun i -> Array.init rowsX ( fun j -> x.[ i , j ] ) |> Array.average ) // sample not population
    
    // de-mean x
    x |> Array2D.iteri ( fun i j e -> x.[ i , j ] <- e - avgs.[ i ] )
    
    let ret = LinearAlgebra.Double.DenseMatrix.OfArray x

    ret.TransposeAndMultiply( ret ).ToArray ( )



let correlation x y = 

    Statistics.Correlation.Pearson( x , y )




let covArrays x y =

    if Array.length x <> Array.length y then

        raise ( Exception "vectors should have the same length" )

    ( correlation x y ) * ( standardDeviation x ) * ( standardDeviation y ) 




let ( .*. ) a b =

    let rowsA , colsA = Array2D.length1 a , Array2D.length2 a
    let rowsB , colsB = Array2D.length1 b , Array2D.length2 b
    
    if colsA <> rowsB then
        
        raise ( [ "mismatch between a and b " ] |> concatFileInfo __LINE__ |> VarianceException )

    let c = Array2D.create rowsA colsB 0.0

    for i in 0 .. rowsA - 1 do
        for j in 0 .. colsB - 1 do
            for k in 0 .. colsA - 1 do
                c.[ i , j ] <- c.[ i , j ] + a.[ i , k ] * b.[ k , j ]

    c




let leastSquares X ( y : float[] ) =

    let dmX = LinearAlgebra.Double.DenseMatrix.Create ( Array2D.length1 X, Array2D.length2 X, System.Func<int, int, float > ( fun i j -> X.[ i , j ] ) )

    let dvY = LinearAlgebra.Double.DenseVector.Create ( y.Length, System.Func<int, float > ( fun i -> y.[i] ) )

    let qr = LinearAlgebra.Double.Factorization.DenseQR ( dmX, LinearAlgebra.Generic.Factorization.QRMethod.Full )

    qr.Solve( dvY ).ToArray
    



let quintiles sortedArray =
    
    let quantileValues = [| 0.2 ; 0.4 ; 0.6 ; 0.8 ; 1.0 |] |> Array.sort // put it in to really stress the point

    quantileValues |> Array.map ( fun tau -> Statistics.SortedArrayStatistics.QuantileCustom( sortedArray , tau , quantileDefinition ) )




let assignScore sortedQuantiles sortedArray =

    let maxScore = Array.length sortedQuantiles

    let numValues = Array.length sortedArray

    let ret : int [] = Array.zeroCreate numValues 

    let quantiles = sortedQuantiles |> Array.map ( fun tau -> Statistics.SortedArrayStatistics.QuantileCustom( sortedArray , tau , quantileDefinition ) )

    let counter = ref 0

    [|

        for i in 0 .. maxScore - 1 ->

            let below  = Array.filter ( fun x -> x <= quantiles.[ i ] ) sortedArray.[ !counter .. numValues - 1 ] 

            counter := !counter + below.Length

            Array.create below.Length ( float i + 1.0 )

    |]

    |> Array.concat




let mode nBuckets rands =

    let hist = Statistics.Histogram( rands, nBuckets )
    let bucketCount = hist.BucketCount

    let mutable maxCount = hist.Item( 0 ).Count
    let mutable i = 0

    for j in 1 .. hist.BucketCount-1 do

        if maxCount < hist.Item( j ).Count then

            maxCount <- hist.Item( j ).Count

            i <- j

    if maxCount = hist.Item( 0 ).Count || maxCount = hist.Item( bucketCount - 1 ).Count then

        raise( Exception( "degenerate distribution" ) )

    let lowerBound = hist.Item( i ).LowerBound
    let upperBound = hist.Item( i ).UpperBound

    rands |> List.filter ( fun r -> lowerBound <= r && r <= upperBound ) |> List.average




let zscore nameValues = 
    
    if Map.isEmpty nameValues then

        Map.empty

    else

        let mean , sd = nameValues |> Map.toList |> List.unzip |> snd |> ( fun l -> ( List.average l , Statistics.Statistics.StandardDeviation l ) )
    
        if sd = 0. then

            raise ( Exception "you passed a constant or an empty map?" )

        nameValues |> Map.map ( fun k v -> ( v - mean ) / sd )




let averageZScore scores =

    let zscores = List.map ( fun score -> zscore score ) scores
    let keys = zscores |> List.map ( fun zscore -> keySet zscore ) |> Set.intersectMany

    [
        for g in keys ->

            let mutable scoreList = List.empty

            for zscore in zscores do

                scoreList <- Map.find g zscore :: scoreList

            ( g , List.average scoreList )

    ]

    |> Map.ofList



    
let rankByQuantiles quantileProbs nameValues =

    let names , values = Map.toArray nameValues |> Array.sortBy ( fun ( _ , v ) -> v ) |> Array.unzip

    assignScore quantileProbs values

        |> Array.mapi ( fun i s -> ( names.[ i ] , s ) ) 
        |> Map.ofArray




let averageQuantilesPerBucket quantileProbs name2Bucket features  =

    let rankPerFeature = 

        features

            |> List.map ( fun m -> 

                            m 
                                |> Map.map ( fun n v -> ( Map.find n name2Bucket , n , v ) )
                                |> Map.toList
                                |> List.unzip
                                |> snd
                                |> Seq.groupBy ( fun ( b , _ , _ ) -> b )
                                |> Seq.map ( fun ( _ , s ) -> s |> Seq.map ( fun ( _ , n , v ) -> ( n , v ) ) |> Map.ofSeq |> rankByQuantiles quantileProbs )
                                |> Seq.toList
                                |> List.fold ( fun s m -> Map.fold ( fun ss k v -> Map.add k v ss ) s m ) Map.empty

                              )
    
    let keys = rankPerFeature |> List.map ( fun scores -> keySet scores ) |> Set.intersectMany

    [
        for g in keys ->

            let mutable scoreList = List.empty

            for score in rankPerFeature do

                scoreList <- Map.find g score :: scoreList

            ( g , List.average scoreList )

    ]

    |> Map.ofList                         
                               
                               


let hmlRanking nameValues = 

    let quantileProbs = [| 1.0 / 3.0 ; 2.0 / 3.0 ; 1.0 |] |> Array.sort // put it in to really stress the point

    let scores = rankByQuantiles quantileProbs nameValues

    scores

        |> Map.fold ( 
                    
                        fun ( lList , mList , hList ) k v -> 

                            if v = 1.0 then

                               ( k :: lList , mList , hList )

                            elif v = 2.0 then

                                ( lList , k :: mList , hList )

                            else

                                ( lList , mList , k :: hList )

                    )
                            
                    ( List.empty , List.empty , List.empty )

        |> ( fun ( l , m , h ) -> [ ( "L" , l ) ; ( "M" , m ) ; ( "H" , h ) ] |> Map.ofList )




let cumsum x =
    
    let ret = Array.init ( Seq.length x ) ( fun i -> 0 )

    ret.[ 0 ] <- Seq.nth 0 x

    for i in 1 .. Seq.length x - 1 do

            ret.[ i ] <- ret.[ i - 1 ] + Seq.nth i x

    ret




let firstPcaComp ( rets : float [ , ] ) =

    let cov = rets |> covariance |> LinearAlgebra.Double.DenseMatrix.OfArray
    
    let evd = new LinearAlgebra.Double.Factorization.DenseEvd ( cov )
    let p = evd.EigenValues( ).ToArray( ) |> Array.map ( fun i -> i.Real ) // real parts
    let ind = p |> Array.reduce ( fun e f -> max e f ) |> ( fun maxEig -> Array.findIndex ( fun e -> e = maxEig ) p )
    let v = evd.EigenVectors ( )
    
    let retsDM = LinearAlgebra.Double.DenseMatrix.OfArray rets
    let firstFactor = v.Column( ind ) |> retsDM.Multiply 

    firstFactor.ToArray ()




let hpFilter lambda data =

    let numObs = Array.length data

    if numObs < 3 then

       data

    else

        let laggedDiff = Array2D.create ( numObs - 2 ) numObs 0.0 |> LinearAlgebra.Double.DenseMatrix.OfArray
        let eye = Array2D.create numObs numObs 0.0 |> LinearAlgebra.Double.DenseMatrix.OfArray
        
        let sqLambda = sqrt lambda
        for i in 0 .. numObs - 3 do

            // alternative to multiply each component by sqLambda is to multiply the matrix laggedDiff.TransposeThisAndMultiply( laggedDiff ) with lambda. didn't find a simple way to do matrix-scalar multiplication
            
            laggedDiff.[ i , i ] <- 1.0 * sqLambda
            laggedDiff.[ i , i + 1 ] <- -2.0 * sqLambda
            laggedDiff.[ i , i + 2 ] <- 1.0 * sqLambda

        for i in 0 .. numObs - 1 do

            eye.[ i , i ] <- 1.0

        let coeffMatrix = eye.Add ( laggedDiff.TransposeThisAndMultiply( laggedDiff ) ) |> LinearAlgebra.Double.DenseMatrix.OfMatrix

        let choleskySolver = LinearAlgebra.Double.Factorization.DenseCholesky( coeffMatrix )

        choleskySolver.Solve( LinearAlgebra.Double.DenseVector data ).ToArray ()        
                



let quadProg doMarketNeutral longs shorts ( upperLongsBounds : float [] ) ( lowerShortsBounds : float [] ) rets =

    // upperlongsBounds should be of length longs or 1 if we want to use one upper bound for all longs. same with upperShortsBounds and lowerShortsBounds

    let numLongs = Array.length longs
    let numShorts = Array.length shorts

    let solver = new InteriorPointSolver () 

    let variableIds = Array.init ( numLongs + numShorts ) ( fun i -> ref 0 )

    // longs

    let mutable success = true
    for i in 0 .. numLongs - 1 do
        success <- success && solver.AddVariable( longs.[ i ] , variableIds.[ i ] )
    if numLongs = Array.length upperLongsBounds then
        for i in 0 .. numLongs - 1 do
            solver.SetBounds ( !variableIds.[ i ] , Rational.op_Implicit 0.0 , Rational.op_Implicit upperLongsBounds.[ i ] )
    else // we passed one bound for all
        for i in 0 .. numLongs - 1 do
            solver.SetBounds ( !variableIds.[ i ] , Rational.op_Implicit 0.0 , Rational.op_Implicit upperLongsBounds.[ 0 ] ) 
    if not success then raise ( Exception "adding long variables failed")

    // shorts

    success <- true
    for i in 0 .. numShorts - 1 do
        success <- success && solver.AddVariable( shorts.[ i ] , variableIds.[ i + numLongs ] )
    if numShorts = Array.length lowerShortsBounds then
        for i in 0 .. numShorts - 1 do
            solver.SetBounds ( !variableIds.[ i + numLongs ] , Rational.op_Implicit lowerShortsBounds.[ i ] , Rational.op_Implicit 0.0 )
    else // we passed one bound for all
        for i in 0 .. numShorts - 1 do
            solver.SetBounds ( !variableIds.[ i + numLongs ] , Rational.op_Implicit lowerShortsBounds.[ 0 ] , Rational.op_Implicit 0.0 )
    if not success then raise ( Exception "adding short variables failed")
    
    // budget constraint

    let budgetConstraintId = ref 0
    success <- solver.AddRow( "budgetConstraint" , budgetConstraintId )
    for i in 0 .. numLongs - 1 do
        solver.SetCoefficient( !budgetConstraintId , !variableIds.[ i ] , Rational.op_Implicit 1.0 )
    for i in 0 .. numShorts - 1 do
        solver.SetCoefficient( !budgetConstraintId , !variableIds.[ i + numLongs ] , Rational.op_Implicit -1.0 )
    solver.SetBounds ( !budgetConstraintId , Rational.op_Implicit 1.0 , Rational.op_Implicit 1.0 )

    // market neutral

    if doMarketNeutral then

        let marketNeutralId = ref 0
        success <- solver.AddRow( "marketNeutral" , marketNeutralId )
        for i in 0 .. variableIds.Length - 1 do
            solver.SetCoefficient( !marketNeutralId , !variableIds.[ i ] , Rational.op_Implicit 1.0 )
        solver.SetBounds ( !marketNeutralId , Rational.op_Implicit 0.0 , Rational.op_Implicit 0.0 )

    // objective

    let objectiveId = ref 0
    success <- solver.AddRow ( "variance" , objectiveId )
    let allInstruments = Array.append longs shorts

    for i in 0 .. allInstruments.Length - 1 do
        for j in 0 .. allInstruments.Length - 1 do
            solver.SetCoefficient( !objectiveId , 
                ( Map.find allInstruments.[ i ] rets , Map.find allInstruments.[ j ] rets ) ||> covArrays |> Rational.op_Implicit , 
                    !variableIds.[ i ] , !variableIds.[ j ] )

    solver.AddGoal( !objectiveId , 0 , true ) |> ignore

    // solve 

    let solution = solver.Solve( new InteriorPointSolverParams () )
    if solution.Result <> LinearResult.Optimal then raise( Exception "QP failed...")

    ( 
        
            [
                for i in 0 .. allInstruments.Length - 1 ->

                    ( allInstruments.[ i ] , solution.GetValue( !variableIds.[ i ] ) |> float )
            ]

            |> Map.ofList ,

            sqrt solver.Statistics.Primal
    )




let minimizeExposureToSystemicFactor ( retsLong : float [ , ] ) ( retsShort : float [ , ] ) ( factor : float [ ] ) =

    let numObs = Array2D.length1 retsLong
    if numObs <> Array2D.length1 retsShort then raise( Exception ( "long and short must have the same number of rows" ) )

    let numLongs = Array2D.length2 retsLong
    let numShorts = Array2D.length2 retsShort

    // Model

    let solverContext = Microsoft.SolverFoundation.Services.SolverContext()

    let model = solverContext.CreateModel()

    // decisions

    let longWeightsDecisions = [| for i in 0 .. numLongs - 1 -> "l_" + i.ToString() |] |> Array.map ( fun n -> Decision ( Domain.RealRange( Rational.op_Implicit 0.0 , Rational.PositiveInfinity ) , n ) )
    let shortWeightsDecisions = [| for i in 0 .. numShorts - 1 -> "s_" + i.ToString() |] |> Array.map ( fun n -> Decision ( Domain.RealRange( Rational.NegativeInfinity , Rational.op_Implicit 0.0 ) , n ) )
    let weightsDecisions = [ longWeightsDecisions ; shortWeightsDecisions ] |> Array.concat

    model.AddDecisions weightsDecisions

    // constraints
////    longWeightsDecisions |> Array.iter( fun d -> model.AddConstraint( d.Name + "_Constraint" , d.Name + " > 0.0 " ) |> ignore )
////    shortWeightsDecisions |> Array.iter( fun d -> model.AddConstraint( d.Name + "_Constraint" , d.Name + " < 0.0 " ) |> ignore )

    model.AddConstraint("budgetConstraint" , weightsDecisions |> Array.map ( fun d -> d.Name ) |> Array.reduce ( fun w0 w1 -> w0 + " + " + w1 ) |> ( fun c -> c + " == 1.0" ) ) |> ignore
    // objective : covariance( Rw , F ) = ( Rw )^T * F / n - (Rw)^T * ones / n * E[F] = w^T * ( R^T * F - R^T * ones * E[F] ) / n

    let rets = Array2D.create numObs ( numLongs + numShorts ) 0.0 
    rets.[ * , 0 .. numLongs - 1 ] <- Array2D.copy retsLong  
    rets.[ * , numLongs ..  ] <- Array2D.copy retsShort 

    let retsMat =  MathNet.Numerics.LinearAlgebra.Double.DenseMatrix.OfArray rets
    let factorVec =  MathNet.Numerics.LinearAlgebra.Double.DenseVector factor
    let onesVec = MathNet.Numerics.LinearAlgebra.Double.DenseVector ( Array.create numObs 1.0 )

    let rTF = retsMat.LeftMultiply factorVec
    let rTOnesF = retsMat.LeftMultiply onesVec
    let meanF = Array.average factor

    rTF.Subtract( rTF , rTOnesF.Multiply meanF )
    let lpArray = rTF.ToArray()
    printfn "rtf %s" (paste lpArray)
    let objective = ( lpArray , weightsDecisions ) ||> Array.map2 ( fun c w -> Term.op_Multiply( Term.op_Implicit ( c ) , w ) ) |> Array.reduce ( fun t0 t1 -> Term.op_Addition ( t0 , t1 ) )
    printfn "objective %A" objective
    let goal = model.AddGoal ( "covariance" , GoalKind.Minimize , objective )

    let directive = SimplexDirective ()
////////    directive.Algorithm <- SimplexAlgorithm.Dual
    printfn "directive %A" directive
    let solution = solverContext.Solve( directive )
    printfn "solution %A" (solution)
    [|
    for decision in solution.Decisions ->
        
        ( decision.Name , decision.ToDouble() )
    |]




let minimizeExposureToSystemicFactor2 ( retsLong : float [ , ] ) ( retsShort : float [ , ] ) ( factor : float [ ] ) =
    
    let numObs = Array2D.length1 retsLong
    
//    if numObs <> Array2D.length1 retsShort then
//        raise( Exception ( "long and short must have the same number of rows" ) )

    let numLongs = Array2D.length2 retsLong
    let numShorts = Array2D.length2 retsShort

    let solver = new SimplexSolver () 
    let longIds = Array.init numLongs ( fun i -> ref 0 ) 
    let shortIds = Array.init numShorts ( fun i -> ref 0 ) 

    for i in 0 .. numLongs-1 do
        
        let succeeded = solver.AddVariable ( "l" + i.ToString() , longIds.[ i ] )
        if not succeeded then
            raise ( Exception ( "error adding long variables" ) )
        printfn "longid %d" !longIds.[i]
        solver.SetBounds( !longIds.[ i ] , Rational.op_Implicit 0.0 , Rational.op_Implicit 1.0 )
    longIds |> Array.iter ( fun i -> printfn "iter %d" !i )
    for i in 0 .. numShorts-1 do
        
        let succeeded = solver.AddVariable ( "s" + i.ToString() , shortIds.[ i ] )
        if not succeeded then
            raise ( Exception ( "error adding short variables" ) )

        solver.SetBounds( !shortIds.[ i ] , Rational.op_Implicit -1.0 , Rational.op_Implicit 0.0 )

    let budgetRowId = ref 0
    let succeededBudgetRow = solver.AddRow( "budgetConstraint" , budgetRowId )
    if not succeededBudgetRow then
            raise ( Exception ( "error adding long variables" ) )
    for i in 0 .. numLongs-1 do
        solver.SetCoefficient( !budgetRowId , !longIds.[ i ] , Rational.op_Implicit 1.0 )
    for i in 0 .. numShorts-1 do
        solver.SetCoefficient( !budgetRowId , !shortIds.[ i ] , Rational.op_Implicit -1.0 )
    solver.SetBounds ( !budgetRowId , Rational.op_Implicit 1.0  , Rational.op_Implicit 1.0 )
    
    let goalId = ref 0
    let succeededGoal = solver.AddRow( "goal" , goalId )
    if not succeededGoal then
            raise ( Exception ( "error adding long variables" ) )
    printfn "ids: \n%s \n%s 1n%d \n%d" (longIds|>Array.map (fun i->i.Value) |>paste ) ( shortIds|>Array.map (fun i->i.Value) |>paste) !goalId !budgetRowId
    let rets = Array2D.create numObs ( numLongs + numShorts ) 0.0 
    rets.[ * , 0 .. numLongs - 1 ] <- Array2D.copy retsLong  
    rets.[ * , numLongs ..  ] <- Array2D.copy retsShort 
    
    let retsMat =  MathNet.Numerics.LinearAlgebra.Double.DenseMatrix.OfArray rets
    let factorVec =  MathNet.Numerics.LinearAlgebra.Double.DenseVector factor
    let onesVec = MathNet.Numerics.LinearAlgebra.Double.DenseVector ( Array.create numObs 1.0 )

    let rTF = retsMat.LeftMultiply factorVec
    let rTOnesF = retsMat.LeftMultiply onesVec
    let meanF = Array.average factor

    rTF.Subtract( rTF , rTOnesF.Multiply meanF )
    let lpArray = rTF.ToArray()
    printfn "%s" (paste lpArray)
    let allVarIds = [ longIds |> Array.map ( fun i -> i.Value ) ; shortIds |> Array.map ( fun i -> i.Value ) ] |> Array.concat
    for i in 0 .. allVarIds.Length-1 do
        solver.SetIntegrality( allVarIds.[ i ] , false )

    for i in 0 .. lpArray.Length-1 do
        solver.SetCoefficient ( !goalId , allVarIds.[ i ] , Rational.op_Implicit lpArray.[ i ] )

    let goal = solver.AddGoal ( !goalId , 1 , true )
    
    let solution = solver.Solve( new SimplexSolverParams () )

    ( solution.GetValue( !goalId ).ToDouble() , solution.GetValue(!budgetRowId).ToDouble(),
        [|

            for i in 0 .. allVarIds.Length - 1 ->
                solution.GetValue( allVarIds.[ i ] ).ToDouble()

        |]
    )
    



    




