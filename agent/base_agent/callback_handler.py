from langchain_core.callbacks import BaseCallbackHandler
from typing import Any, Dict, List
import json


class DeepSeekReasoningCallbackHandler(BaseCallbackHandler):
    """Custom callback handler for DeepSeek reasoner model to capture reasoning_content."""
    
    def __init__(self):
        super().__init__()
        self.reasoning_content = ""
        self.final_content = ""
    
    def on_llm_new_token(self, token: str, **kwargs: Any) -> None:
        """Handle new tokens from the LLM."""
        # This method is called for each new token in streaming mode
        # We'll accumulate tokens here, though for DeepSeek we need to handle chunks differently
        pass
    
    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        """Handle the end of LLM generation."""
        # Reset content for next call
        self.reasoning_content = ""
        self.final_content = ""
    
    def extract_reasoning_content(self, chunk: Any) -> str:
        """Extract reasoning content from streaming chunk.
        
        Args:
            chunk: Streaming response chunk from DeepSeek API
            
        Returns:
            Extracted reasoning content string
        """
        reasoning_content = ""
        try:
            if hasattr(chunk, 'choices') and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if hasattr(delta, 'reasoning_content') and delta.reasoning_content:
                    reasoning_content = delta.reasoning_content
        except Exception as e:
            print(f"Warning: Failed to extract reasoning content: {e}")
        
        return reasoning_content
    
    def extract_final_content(self, chunk: Any) -> str:
        """Extract final content from streaming chunk.
        
        Args:
            chunk: Streaming response chunk from DeepSeek API
            
        Returns:
            Extracted final content string
        """
        final_content = ""
        try:
            if hasattr(chunk, 'choices') and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if hasattr(delta, 'content') and delta.content:
                    final_content = delta.content
        except Exception as e:
            print(f"Warning: Failed to extract final content: {e}")
        
        return final_content