import faiss
import pickle

with open('embedding_dict.pickle', 'rb') as f:
    data = pickle.load(f)

class ExactIndex():
    def __init__(self, vectors, labels):
        self.dimension = vectors.shape[1]
        self.vectors = vectors.astype('float32')
        self.labels = labels    
   
    def build(self):
        self.index = faiss.IndexFlatL2(self.dimension,)
        self.index.add(self.vectors)
        
    def query(self, vectors, k=200):
        distances, indices = self.index.search(vectors, k)
        print(distances) 
        # I expect only query on one vector thus the slice
        return [self.labels[i] for i in indices[0]]
    
index = ExactIndex(data["embedding"], data["unique_id"])
index.build()
faiss.write_index(index.index, "index.faiss")
faiss.inde
print(index.query(data['embedding'][:1]))