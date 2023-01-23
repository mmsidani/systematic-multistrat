module SA2.Common.Kalman

// NOTE:based on example from tryfsharp.org


open System
open MathNet.Numerics

open MathNet.Numerics.LinearAlgebra.Double
open MathNet.Numerics.LinearAlgebra.Generic

type KalmanResult = 

/// Represents the results of single iteration 

  { LogLikelihood : float 
    Values : DenseVector list
    Variances : DenseMatrix list }

  /// Updates the state after a single iteration
  /// (the loglik value is added to the log likelihood,
  /// and 'value' with 'variance' are added to lists)
  member x.Update(loglik, value, variance) =

    { LogLikelihood = x.LogLikelihood + loglik
      Values = value::x.Values
      Variances = variance::x.Variances }

  /// Reverse the accumulated values and variances
  member x.Reverse() =

    { x with Values = List.rev x.Values
             Variances = List.rev x.Variances }

  /// Initial result with no values and 0 likelihood              
  static member Initial = 

    { LogLikelihood = 0.0
      Values = []; Variances = []; }



let symmetrize ( m:DenseMatrix ) = 

    (m + m.Transpose()).Divide(2.0) |> DenseMatrix.OfMatrix


// Identity matrix of size indexCount*indexCount
let idMatrix count =  

    new DenseMatrix( count , count ,
                        [| for i in 0 .. count - 1 ->
                            [| for j in 0 .. count - 1 ->
                                if i = j then 1.0 else 0.0 |] |] |> Array.concat )                             



let logLikelihoodChange (observedValue:DenseVector) (projectedValue:DenseVector ) ( projectedVar : DenseMatrix ) noiseR numObs =

/// Calculates the change of the log likelihood after an update.
/// Estimates the likelihood of the actual 'observedValue' with respect
/// to our 'projectedValue' given 'projectedVar' variance


  // Projected probability with added noise
  let S = projectedVar + noiseR

  // Calculate log likelihood change
  let d = observedValue - projectedValue 

  -(float numObs)*0.5*log(2.0*Math.PI) - 0.5*log(S.Determinant()) - 0.5*(d * S.Inverse())*d



let rec kalmanFilter timeStep (results:KalmanResult) (dynamics:DenseMatrix) ( observedData : DenseMatrix )  noiseR noiseQ =

/// Calculates values projected by the Kalman filter model

  if timeStep = observedData.ColumnCount then 

    // Return finalized results after completion
    results.Reverse()

  else

    // Get previous value and variance (start with the
    // first value from the data set and just noise)
    let pastValue, pastVar = 

        if timeStep = 0 then  
      
            DenseVector.OfVector(observedData.Column(0)) , noiseQ
        else 
      
            List.head results.Values, List.head results.Variances

    // Calculate projected value and its variance
    let projectedValue = dynamics * pastValue
    let projectedVar = dynamics * pastVar * dynamics.Transpose() + noiseQ |> DenseMatrix.OfMatrix

    // Calculate 'Kalman gain' and update the values using observed data
    let observedValue = observedData.Column(timeStep) |> DenseVector.OfVector
    let kalmanGain = projectedVar * (projectedVar + noiseR).Inverse() |> DenseMatrix.OfMatrix
    let update = kalmanGain * (observedValue - projectedValue)
    let nextValue = projectedValue + update
    let nextVar = projectedVar - kalmanGain * projectedVar |> symmetrize

    // Compute the state for the next step of the iteration
    let logChange = logLikelihoodChange observedValue projectedValue projectedVar noiseR observedData.RowCount
    let results = results.Update(logChange, nextValue, nextVar)

    kalmanFilter (timeStep + 1) results dynamics observedData noiseR noiseQ
    


let expectationStep dynamics observedData noiseR noiseQ =

/// Represents 'expectation' step of the algorithm
/// (Run Kalman filter to get log-likelihood and values)

  kalmanFilter 0 KalmanResult.Initial dynamics observedData noiseR noiseQ



let maximizationStep param (dynamics:DenseMatrix ) observedCount =

/// Represents the 'maximization' step of the algorithm
/// (Compute new value of parameters to maximize likelihood)

  // Transform hidden values into a matrix & get components
  let undVector = param.Values |> List.toArray |>  Array.fold ( fun ret ( x : DenseVector ) -> x.ToArray() |> Array.append  ret  ) Array.empty
  let hiddenVals = new DenseMatrix( param.Values.[0].Count, param.Values.Length,  undVector )
  let hiddenPrev = hiddenVals.SubMatrix(0, hiddenVals.RowCount, 0 , observedCount - 1)
  let hiddenNext = hiddenVals.SubMatrix(0, hiddenVals.RowCount, 1 , observedCount - 1 )

  // Calculate variance between two neighboring elements
  let crossVar = 

    Seq.pairwise param.Variances
    |> Seq.map (fun (pastVar, nextVar) -> 
        (dynamics * pastVar).Transpose() * 
          ((dynamics * pastVar) * dynamics.Transpose()).Inverse() * 
            nextVar)
    |> Seq.reduce (+) 

  // Sum variance matrices excluding the last one
  let vars = 

    param.Variances 
    |> Seq.take (observedCount - 1) |> Seq.reduce (+)

  // Calculate new value of 'dynamics' parameter
  let h1 = (hiddenPrev * hiddenNext.Transpose()) + crossVar
  let h2 = (hiddenPrev * hiddenPrev.Transpose()) + vars

  DenseMatrix.OfMatrix( h1.Transpose() * h2.Inverse() )



let fitModel maxIter ( noise : float ) ( observedDataArray : float[ , ] ) =

/// Repeatedly runs expectation and maximization step of
/// the EM algorithm to calculate the 'dynamics' parameter

    let observedData = DenseMatrix.OfArray( observedDataArray )

    // Compute two diagonal matrices with the noise
    let noiseQ = noise * idMatrix observedData.RowCount
    let noiseR = noise * idMatrix observedData.RowCount

    /// Inner recursive function that is called recursively
    let rec updateModel oldParam (logliks:float list) iter observedData =

    // Run a single step of the EM algorithm
        let results = expectationStep oldParam observedData noiseR noiseQ
        let newParam = maximizationStep results oldParam observedData.ColumnCount

        // Check for convergence
        let logliks = results.LogLikelihood::logliks
        match logliks with
        | current::past::_ when current - past < 0.1 -> 
            newParam, List.rev logliks, Some iter
        | _ when iter > maxIter ->
            newParam, List.rev logliks, None
        | _ ->
            updateModel newParam logliks (iter + 1) observedData

    // Start the estimation with the identity matrix
    updateModel ( idMatrix observedData.RowCount ) [] 1 observedData



let predictUpdate =

    ()



let private kalmanTest maxIters observedData noise =

// Train the model with at most 50 iterations
    let dynamics, logliks, converged = fitModel maxIters noise observedData

    // Report whether the training has converged
    match converged with
    | Some iter -> printfn "Converged in iteration %i" iter
    | _ -> printfn "Not converged."

    // Print interactions between pairs of stock markets
   
    // Get a list of interactions between stock markets
    let interactions = 

      [ for i in 0 .. Array2D.length1 observedData - 1 do
          for j in 0 .. Array2D.length1 observedData - 1 do
            if i <> j && (abs dynamics.[i, j]) > 0.2 then
              yield i, j ]

    // Calculate X and Y positions of points for every stock market
    let q = 6.28 / float ( Array2D.length1 observedData )

    let indexPoints = [| for x in 0.0 .. q .. 6.28 -> sin x, cos x |]

    ()
