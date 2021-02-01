test_predictions = cv_model.transform(test_sdf)

evaluator = BinaryClassificationEvaluator()
print('ROC AUC: test', evaluator.evaluate(test_predictions))
print(evaluator.explainParams())  # Available metrics.
