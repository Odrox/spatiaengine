"""
Progress Display Module for SpatiaEngine
Provides user-friendly progress tracking and visualization
"""
import sys
from typing import Optional, Dict, Any
from datetime import datetime
import time

class ProgressDisplay:
    """Enhanced progress display with visual feedback."""
    
    def __init__(self, total_steps: int = 0):
        """
        Initialize progress display.
        
        Args:
            total_steps: Total number of steps in the process
        """
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = time.time()
        self.step_descriptions = []
        self.completed_steps = set()
        
    def start_process(self, title: str):
        """Start the main process display."""
        print(f"\n{'='*60}")
        print(f"🚀 {title}")
        print(f"{'='*60}")
        if self.total_steps > 0:
            print(f"📋 Total steps: {self.total_steps}")
        print()
        
    def add_step(self, description: str):
        """Add a step to track."""
        self.step_descriptions.append(description)
        
    def start_step(self, step_num: int, description: str = ""):
        """
        Start a step.
        
        Args:
            step_num: Step number (1-based)
            description: Optional step description
        """
        self.current_step = step_num
        desc = description or (self.step_descriptions[step_num-1] if step_num <= len(self.step_descriptions) else f"Step {step_num}")
        
        # Clear previous line and show current step
        sys.stdout.write(f"\r\033[K🔄 {desc}... ")
        sys.stdout.flush()
        
    def complete_step(self, step_num: int = None, success: bool = True, message: str = ""):
        """
        Mark a step as completed.
        
        Args:
            step_num: Step number (defaults to current step)
            success: Whether step completed successfully
            message: Optional completion message
        """
        step = step_num or self.current_step
        if step > 0:
            self.completed_steps.add(step)
            
        # Show completion status
        status = "✅" if success else "❌"
        desc = self.step_descriptions[step-1] if step <= len(self.step_descriptions) else f"Step {step}"
        
        if message:
            print(f"\r\033[K{status} {desc} - {message}")
        else:
            print(f"\r\033[K{status} {desc}")
            
    def show_progress_bar(self, current: int, total: int, prefix: str = "", suffix: str = ""):
        """
        Show a progress bar.
        
        Args:
            current: Current progress
            total: Total amount
            prefix: Text before progress bar
            suffix: Text after progress bar
        """
        bar_length = 30
        progress = current / total if total > 0 else 0
        filled_length = int(bar_length * progress)
        bar = "█" * filled_length + "░" * (bar_length - filled_length)
        
        percent = f"{progress * 100:.1f}"
        sys.stdout.write(f"\r{prefix} |{bar}| {percent}% {suffix}")
        sys.stdout.flush()
        
    def finish_process(self, success: bool = True, message: str = ""):
        """
        Finish the process display.
        
        Args:
            success: Whether process completed successfully
            message: Optional final message
        """
        elapsed_time = time.time() - self.start_time
        minutes, seconds = divmod(int(elapsed_time), 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        
        print(f"\n{'='*60}")
        if success:
            print(f"🎉 Process completed successfully in {time_str}!")
        else:
            print(f"⚠️  Process completed with issues in {time_str}.")
            
        if message:
            print(f"📝 {message}")
            
        print(f"{'='*60}\n")
        
    def show_summary(self, results: Dict[str, Any]):
        """
        Show process summary.
        
        Args:
            results: Dictionary of results to display
        """
        print("📊 Process Summary:")
        print("-" * 40)
        for key, value in results.items():
            print(f"  {key}: {value}")
        print()

# Global instance for easy access
progress_display = ProgressDisplay()