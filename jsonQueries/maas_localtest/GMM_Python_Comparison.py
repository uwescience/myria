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

def run_gmm_test(n_steps):

    # Create mock dataset.
    #x = np.array([1., 1.5, 3.0, 3.5, 4.0])
    #y = np.array([1., 0.8, 3.5, 3.2, 4.2])
    #data = np.column_stack((x,y))

    a = np.random.randn(5,4) + 7
    b = np.random.randn(5,4) + -4
    data = np.row_stack((a,b)) 

    #data = np.loadtxt('astro_sample.csv',delimiter=',')

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
        gmm = GMM(n_components=3, n_iter=j, n_init=1, random_state=0, covariance_type='full', min_covar=1.e-14)
        gmm.fit(data)
        responsibilities = gmm.predict_proba(data)
        resp = responsibilities
        weights = gmm.weights_
        means = gmm.means_
        cov = gmm.covars_
        

        if j == 0:        
            ##### Test input, the points and responsibilities
            combined_points = np.concatenate((data,resp), axis=1)            

            f_log.write('Initial points and responsibilities\n')        
            write_data(f_log, combined_points)
            f_log.write('\n')        
            
            with open('rawpointsonly.csv','w') as f:
                write_data(f, combined_points)
            with open('PointsOnly.csv','w') as f:
                write_data(f, combined_points)
                
        if j == 1:
            ##### Test input, the gaussian parameters
            K = means.shape[0]
            D = means.shape[1]
            combined_components = np.concatenate(
                            (weights.reshape(K,1), means, cov.reshape(K, D*D)), axis=1)
            
            # Write log entry
            f_log.write('Test Gaussian parameters\n')
            write_data(f_log, combined_components)
            f_log.write('\n')        

            # Write test file
            with open('rawcomponentsonly.csv','w') as f:
                write_data(f, combined_components)
            with open('ComponentsOnly.csv','w') as f:
                write_data(f, combined_components)


        if j == n_steps:
            #### Expected output, the responsibilities after one step
            combined_points = np.concatenate((data,resp), axis=1)            
            
            # Write log entry
            f_log.write('\nExpected point responsibilities\n')
            write_data(f_log, combined_points)  
            f_log.write('\n')   
            
            # Write test file
            with open('raw_expected_points.csv','w') as f:
                write_data(f, combined_points)
            with open('ExpectedPoints.csv','w') as f:
                write_data(f, combined_points)

        
        if j == n_steps + 1:
            #### Expected output, the gaussian parameters after one step
            K = means.shape[0]
            D = means.shape[1]
            combined_components = np.concatenate(
                    (weights.reshape(K,1), means, cov.reshape(K, D*D)), axis=1)

            # Write log entry
            f_log.write('\nExpected Gaussian parameters\n')
            write_data(f_log, combined_components)
            f_log.write('\n') 
            
            # Write test file
            with open('raw_expected_components.csv','w') as f:
                write_data(f, combined_components)
            with open('ExpectedComponents.csv','w') as f:
                write_data(f, combined_components)

    f_log.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "No number of iterations passed in, using 1 EM step."
        num_iterations = 1
    else:
        num_iterations = int(sys.argv[1])
    
    run_gmm_test(num_iterations)
