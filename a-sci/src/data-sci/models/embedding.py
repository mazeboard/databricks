import mlflow

class EmbeddingModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        from FlagEmbedding import BGEM3FlagModel
        self.model = BGEM3FlagModel('BAAI/bge-m3', use_fp16=True)

    def predict(self, context, model_input):
        return self.model.encode(model_input, return_dense=True)["dense_vecs"]
