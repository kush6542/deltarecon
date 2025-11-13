"""
ASCII Art Banner for DeltaRecon Framework

This module contains the ASCII art banner displayed at the start of validation runs.
To change the banner style, simply replace the BANNER constant below.
"""

# Main banner - displayed at the start of each validation run
BANNER = """
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   ██████╗ ███████╗██╗  ████████╗ █████╗                   ║
║   ██╔══██╗██╔════╝██║  ╚══██╔══╝██╔══██╗                  ║
║   ██║  ██║█████╗  ██║     ██║   ███████║                  ║
║   ██║  ██║██╔══╝  ██║     ██║   ██╔══██║                  ║
║   ██████╔╝███████╗███████╗██║   ██║  ██║                  ║
║   ╚═════╝ ╚══════╝╚══════╝╚═╝   ╚═╝  ╚═╝                  ║
║                                                           ║
║   ██████╗ ███████╗ ██████╗ ██████╗ ███╗   ██╗             ║
║   ██╔══██╗██╔════╝██╔════╝██╔═══██╗████╗  ██║             ║
║   ██████╔╝█████╗  ██║     ██║   ██║██╔██╗ ██║             ║
║   ██╔══██╗██╔══╝  ██║     ██║   ██║██║╚██╗██║             ║
║   ██║  ██║███████╗╚██████╗╚██████╔╝██║ ╚████║             ║
║   ╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝             ║
║                                                           ║
║   Version: v1.0.0                                         ║
║   Author:  kushagra.parashar@databricks.com               ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
"""


def get_banner() -> str:
    """
    Get the DeltaRecon ASCII art banner.
    
    Returns:
        str: The ASCII art banner string
    """
    return BANNER


def print_banner() -> None:
    """
    Print the DeltaRecon ASCII art banner to stdout.
    
    This is called at the start of each validation run to display
    a visual indicator that the framework is starting.
    """
    print(BANNER)

