
class DataProject:

    def __init__(self, id):
        self.id = id

    def getConfiguration(self):
        return {'content_cluster_components': {'value': 2},
            'content_clusters': {'value': 20},
            'content_dim_components': {'value': 2},
            'dimensions': {'value': [{'label': 'Username'},
                {'label': 'Date'},
                {'label': 'Lang'}]},
            'file_format': {'value': {'encoding': 'UTF-8',
                'delimiter': ';',
                'quote': '"',
                'quoteEscape': '"',
                'escape': '"'}},
            'file_mapping': {'value': [{'type': 'text', 'input': 'text'},
                {'type': 'text', 'input': 'original id'},
                {'type': 'text', 'input': 'user'},
                {'type': 'date', 'input': 'date', 'format': 'dd/MM/yyyy hh:mm'},
                {'type': 'text', 'input': 'lang'}]},
            'tags': {},
            'term_black_list': {},
            'topic_label_prompt': {'value': 'Summarize the content of the following list of sentences in French'},
            'topic_medium_label_prompt': {'value': 'Summarize in a few words the subject of the following text in French'},
            'topic_small_label_prompt': {'value': 'Summarize in maximum 5 words the following text in French'}}
