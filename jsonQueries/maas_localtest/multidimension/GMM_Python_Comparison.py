# coding: utf-8
"""
Ryan Maas and Grace Telford
CSE 544 Project

Script to perform fixed number of GMM iterations on simple dataset
and write weights, means, covariances, and responsibilities to file.
"""

import sys
import numpy as np
from sklearn.mixture import GMM

num_random_points_per_worker = 10000

def write_data(f, data_array):
    n_rows = data_array.shape[0]
    n_parameters = data_array.shape[1]
    for i in range(n_rows):
        f.write("%s, " % i)
        for h in range(n_parameters):
            f.write("%s" % data_array[i,h])
            if h == n_parameters - 1:
                f.write("\n")
            else:
                f.write(", ")

def run_gmm_test(n_steps, num_dimensions, n_components, logging=False):

    # Create mock dataset.
    #x = np.array([1., 1.5, 3.0, 3.5, 4.0])
    #y = np.array([1., 0.8, 3.5, 3.2, 4.2])
    #data = np.column_stack((x,y))

    a = np.random.randn(num_random_points_per_worker, num_dimensions) + 7
    b = np.random.randn(num_random_points_per_worker, num_dimensions) + -4
    data = np.row_stack((a,b)) 

    #data = np.loadtxt('astro_sample.csv',delimiter=',')

    if logging:
        f_log = open('gmm_test_log.txt','w')

    # Run GMM and save output after desired # iterations to file.
    # Automatically initialized st each component has zero mean and 
    # identity covariance and weights are equal.

    n_iterations = [0, 1, n_steps, 1 + n_steps]


    resp = []
    weights = []
    means = []
    cov = []


    for j in n_iterations:
        # GMM code goes here
        gmm = GMM(n_components=n_components, n_iter=j, n_init=1, random_state=0, covariance_type='full', min_covar=1.e-14)
        gmm.fit(data)
        responsibilities = gmm.predict_proba(data)
        resp = responsibilities
        weights = gmm.weights_
        means = gmm.means_
        cov = gmm.covars_
        

        if j == 0:        
            ##### Test input, the points and responsibilities
            combined_points = np.concatenate((data,resp), axis=1)            

            if logging:
                f_log.write('Initial points and responsibilities\n')        
                write_data(f_log, combined_points)
                f_log.write('\n')        
            
            with open('PointsOnly.csv','w') as f:
                write_data(f, combined_points)
                
        if j == 1:
            ##### Test input, the gaussian parameters
            K = means.shape[0]
            D = means.shape[1]
            combined_components = np.concatenate(
                            (weights.reshape(K,1), means, cov.reshape(K, D*D)), axis=1)
            
            if logging:
                # Write log entry
                f_log.write('Test Gaussian parameters\n')
                write_data(f_log, combined_components)
                f_log.write('\n')        

            # Write test file
            with open('ComponentsOnly.csv','w') as f:
                write_data(f, combined_components)


        if j == n_steps:
            #### Expected output, the responsibilities after one step
            combined_points = np.concatenate((data,resp), axis=1)            
            
            if logging:
                # Write log entry
                f_log.write('\nExpected point responsibilities\n')
                write_data(f_log, combined_points)  
                f_log.write('\n')   
            
            # Write test file
            with open('ExpectedPoints.csv','w') as f:
                write_data(f, combined_points)

        
        if j == n_steps + 1:
            #### Expected output, the gaussian parameters after one step
            K = means.shape[0]
            D = means.shape[1]
            combined_components = np.concatenate(
                    (weights.reshape(K,1), means, cov.reshape(K, D*D)), axis=1)

            # Write log entry
            if logging:
                f_log.write('\nExpected Gaussian parameters\n')
                write_data(f_log, combined_components)
                f_log.write('\n') 
            
            # Write test file
            with open('ExpectedComponents.csv','w') as f:
                write_data(f, combined_components)

    if logging:
        f_log.close()


if __name__ == "__main__":
    num_iterations = 1    
    num_dimensions = 4
    num_components = 7
    if len(sys.argv) == 1:
        print "No number of iterations passed in, using 1 EM step."
    elif len(sys.argv) == 2:
        num_iterations = int(sys.argv[1])        
        print "One argument passed; using number of interations %d" % (num_iterations)
    elif len(sys.argv) == 4:
        num_iterations = int(sys.argv[1]) 
        num_dimensions = int(sys.argv[2])  
        num_components = int(sys.argv[3])        
        print ("Three arguments passed; using %d iterations, %d dimensions, and %d components." %
                (num_iterations, num_dimensions, num_components))
    else:
        print "Bad input. Input is the following:"
        print "\tArg 1: num_iterations"
        print "\tArg 2: num_dimensions"
        print "\tArg 3: num_components"
        print ("If no arguments, defaults to 1 iteration. If no dimensions and components, " + 
                "defaults to 4 dimensions and 7 components. Always uses 20,000 test points")
    run_gmm_test(num_iterations, num_dimensions, num_components)
