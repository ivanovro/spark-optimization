from pyspark.ml.clustering import GaussianMixture

g = sns.lmplot(x='X Coordinate', y='Y Coordinate', hue='Primary Type', data=crime_df,
               fit_reg=False, size=10, palette={'NARCOTICS': 'tomato', 'THEFT': 'skyblue'})

# for each type of crime
for crime_type, colour in [('NARCOTICS', 'r'), ('THEFT', 'b')]:
    crime_subset = (
        crime_with_features_sdf
        .filter(sf.col('Primary Type') == crime_type)
    )

    # fit a GMM
    gmm = GaussianMixture(k=30)
    model = gmm.fit(crime_subset)

    # extract the centers of the gaussians
    centers = (
        model
        .gaussiansDF
        .toPandas()
    )

    # Put the transformed data in a variable below
    crimes_with_predictions = model.transform(crime_subset)

    # 2.
    # Write code here
    ranked_gaussians = (
        crimes_with_predictions
        .withColumn('probability', get_probability_udf('probability'))
        .groupby('prediction', 'Primary Type')
        .agg(sf.sum('probability').alias('probability'))
        .sort('probability', ascending=False)
    )

    # 3.
    # Write code here
    best_gaussians = (
        ranked_gaussians
        .limit(5)
        .toPandas()
        ['prediction']
        .values
    )

    # plot centers on map
    for j in best_gaussians:
        x, y = centers.loc[j, 'mean']
        g.ax.plot(x, y, colour + 's')
