import chromadb
from sentence_transformers import SentenceTransformer

# Load the index
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_collection("y14m_docs")
model = SentenceTransformer('all-MiniLM-L6-v2')

def ask(question: str) -> str:
    """Answer a question using the document index."""
    try:
        # Get relevant documents
        question_embedding = model.encode([question])[0]
        results = collection.query(
            query_embeddings=[question_embedding.tolist()],
            n_results=3
        )
        
        docs = results["documents"][0]
        
        # Extract best matching sentence
        sentences = []
        for doc in docs:
            sentences.extend([s.strip() + '.' for s in doc.split('.') if s.strip()])
        
        if not sentences:
            return "No relevant information found in documents."
        
        # Simple word overlap scoring
        q_words = set(question.lower().split())
        best_sentence = max(
            sentences, 
            key=lambda s: len(q_words.intersection(set(s.lower().split())))
        )
        
        return f"Based on the documentation: {best_sentence}"
    
    except Exception as e:
        return f"Error retrieving answer: {str(e)}"

# Test it
if __name__ == "__main__":
    print(ask("What is Y-14M?"))
