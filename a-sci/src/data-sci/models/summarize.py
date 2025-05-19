import mlflow

class SummarizeModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        from gritlm import GritLM
        self.model = GritLM("GritLM/GritLM-7B", torch_dtype="auto")

    def predict(self, context, model_input):
        def predict_prompt(prompt, content, max):
            messages = [
                {"role": "user", "content": f"{prompt}: {content}"}
            ]
            encoded = self.model.tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(self.model.device)
            gen = self.model.generate(encoded, max_new_tokens=max, do_sample=False)
            decoded = self.model.tokenizer.batch_decode(gen)
            return decoded[0].split('<|assistant|>\n')[1].split('</s>')[0]

        large = predict_prompt(model_input['large_prompt'], model_input['text'], 5000)
        medium = predict_prompt(model_input['medium_prompt'], large, 1000)
        small = predict_prompt(model_input['small_prompt'], medium, 100)

        return { 'large': large, 'medium': medium, 'small': small }
    