import sys
import time
import subprocess
import signal
from pathlib import Path
from finetune.supervisor.manager import SupervisorManager
from finetune.config import Config

try:
    from rich import print as rprint
except ImportError:
    rprint("❌ [red]rich is required[/red]")
    sys.exit(1)

try:
    from typing_extensions import Annotated
except ImportError:
    rprint("❌ [red]typing_extensions is required[/red]")
    sys.exit(1)

try:
    import typer
except ImportError:
    rprint("❌ [red]typer is required[/red]")
    sys.exit(1)

from finetune.cli import console, ConfigOption, VerboseOption

def register_start(app):
    @app.command()
    def start(
        config: ConfigOption = None,
        verbose: VerboseOption = False,
        detach: Annotated[
            bool,
            typer.Option(
                "--detach", "-d",
                help="Run in detached mode (don't follow logs)"
            )
        ] = False,
        force: Annotated[
            bool,
            typer.Option(
                "--force", "-f",
                help="Force restart of already running processes"
            )
        ] = False
    ):
        """
        Start all processes.
        
        By default, this command will start processes and follow the supervisor logs.
        Use --detach/-d to start processes in the background without following logs.
        
        This command will:
        1. Check if processes are already running
        2. Generate supervisor configuration if needed
        3. Start the supervisor daemon
        4. Start all configured processes
        5. Follow supervisor logs (unless --detach is used)
        
        Use --verbose for detailed process status information.
        Use --force to restart already running processes.
        Use --detach/-d to run in background without following logs.
        """
        app_config = Config.load(config) if config else Config()
        manager = SupervisorManager(app_config)

        # Check Redis availability before starting processes
        from finetune.utils.redis_health import RedisHealthChecker
        redis_checker = RedisHealthChecker(app_config.redis)

        if not redis_checker.ensure_redis_available(auto_start=True, allow_install=not detach): 
            if not detach:  # Only show detailed help in non-detached mode
                rprint("\n[red]❌ Cannot start processes without Redis[/red]")
                redis_checker.show_redis_help()
                rprint("\n[yellow]Try running one of the above commands, then run:[/yellow]")
                rprint("  [cyan]finetune start[/cyan]")
            raise typer.Exit(1)

        # Initialize needs_starting
        needs_starting = False
        
        # Get expected processes from config (for verbose mode)
        if verbose:
            expected_processes = set(app_config.get_process_configs().keys())
        
        # Check current status before starting
        try:
            status_info = manager.get_status()
            
            if status_info:
                # Check which processes are already running
                running_processes = [
                    name for name, info in status_info.items() 
                    if info.get('statename') == 'RUNNING'
                ]
                
                stopped_processes = [
                    name for name, info in status_info.items() 
                    if info.get('statename') in ['STOPPED', 'EXITED', 'FATAL']
                ]
                
                if verbose:
                    # Detailed verbose output
                    current_processes = set(status_info.keys())
                    
                    # Categorize processes for verbose mode
                    running_processes_dict = {
                        name: info for name, info in status_info.items()
                        if info.get('statename') == 'RUNNING'
                    }
                    
                    stopped_processes_dict = {
                        name: info for name, info in status_info.items()
                        if info.get('statename') in ['STOPPED', 'EXITED', 'FATAL']
                    }
                    
                    missing_processes = expected_processes - current_processes
                    
                    # Show detailed current state
                    if running_processes_dict:
                        rprint(f"[green]🟢 Running processes ({len(running_processes_dict)}):[/green]")
                        for name, info in running_processes_dict.items():
                            pid = info.get('pid', 'N/A')
                            rprint(f"  • {name} (PID: {pid})")
                    
                    if stopped_processes_dict:
                        rprint(f"[yellow]🟡 Stopped processes ({len(stopped_processes_dict)}):[/yellow]")
                        for name in stopped_processes_dict:
                            rprint(f"  • {name}")
                    
                    if missing_processes:
                        rprint(f"[red]🔴 Missing processes ({len(missing_processes)}):[/red]")
                        for name in missing_processes:
                            rprint(f"  • {name}")
                    
                    # Handle force restart in verbose mode
                    if force and running_processes_dict:
                        rprint("[yellow]🔄 Force restart requested for running processes[/yellow]")
                        for proc_name in running_processes_dict:
                            try:
                                manager.restart_process(proc_name)
                                rprint(f"  • ✅ Restarted {proc_name}")
                            except Exception as e:
                                rprint(f"  • ❌ Failed to restart {proc_name}: {e}")
                
                else:
                    # Simple output for non-verbose mode
                    if running_processes:
                        rprint(f"⚠️  Found {len(running_processes)} already running process(es):")
                        for proc_name in running_processes:
                            rprint(f"  • [green]{proc_name}[/green] (already running)")
                    
                    if stopped_processes:
                        rprint(f"⚠️  Found {len(stopped_processes)} stopped process(es) to start:")
                        for proc_name in stopped_processes:
                            rprint(f"  • [yellow]{proc_name}[/yellow] (will be started)")
                    
                    # Handle force restart in simple mode
                    if force and running_processes:
                        rprint("[yellow]🔄 Force restart requested[/yellow]")
                        for proc_name in running_processes:
                            try:
                                manager.restart_process(proc_name)
                                if verbose:
                                    rprint(f"  • ✅ Restarted {proc_name}")
                            except Exception as e:
                                rprint(f"  • ❌ Failed to restart {proc_name}: {e}")
                        if not verbose:
                            rprint("✅ Processes restarted")
                
                # Check if we need to start anything
                if verbose:
                    needs_starting = stopped_processes_dict or missing_processes or not status_info
                else:
                    needs_starting = stopped_processes or not status_info
                
                if not needs_starting and not force:
                    if not detach:
                        rprint("✅ All processes are already running")
                        _follow_supervisor_logs(app_config, manager)
                    # In detached mode, exit silently if all processes are running
                    return
                
        except Exception:
            # If we can't get status, supervisor probably isn't running yet
            if verbose:
                rprint("[dim]Supervisor not running, starting fresh...[/dim]")
            else:
                rprint("[dim]Supervisor daemon not running, will start fresh[/dim]")
            needs_starting = True
        
        # Start supervisor/processes
        if needs_starting or not status_info:
            if not detach:
                # Show status messages only when not in detached mode
                with console.status("[green]Starting supervisor processes...", spinner="dots"):
                    try:
                        manager.start(daemon=True)  # Always start supervisor in daemon mode
                        
                        if verbose:
                            # Get final status to show what actually started
                            final_status = manager.get_status()
                            running_count = sum(
                                1 for info in final_status.values() 
                                if info.get('statename') == 'RUNNING'
                            )
                            rprint(f"\n✅ Supervisor started with {running_count} running process(es)")
                        else:
                            rprint("\n✅ Supervisor started successfully")
                        
                    except Exception as e:
                        error_msg = str(e).lower()
                        
                        # Handle common "already running" scenarios gracefully
                        if any(phrase in error_msg for phrase in [
                            "already listening",
                            "already running", 
                            "already started",
                            "bind address already in use"
                        ]):
                            rprint("⚠️  Supervisor was already running")
                            
                            # Try to get current status
                            try:
                                status_info = manager.get_status()
                                running_count = sum(
                                    1 for info in status_info.values() 
                                    if info.get('statename') == 'RUNNING'
                                )
                                rprint(f"✅ Found {running_count} running process(es)")
                            except Exception:
                                rprint("✅ Processes are running")
                                
                        else:
                            # This is a real error
                            rprint(f"❌ Failed to start processes: {e}")
                            raise typer.Exit(1)
            else:
                # Detached mode: start silently
                try:
                    manager.start(daemon=True)
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Only show errors in detached mode, not "already running" messages
                    if not any(phrase in error_msg for phrase in [
                        "already listening",
                        "already running", 
                        "already started",
                        "bind address already in use"
                    ]):
                        # This is a real error - show it even in detached mode
                        rprint(f"❌ Failed to start processes: {e}")
                        raise typer.Exit(1)
        
        # Follow supervisor logs (unless detached)
        if not detach:
            _follow_supervisor_logs(app_config, manager)
        else:
            # Final status report for detached mode
            try:
                final_status = manager.get_status()
                if final_status:
                    running_final = sum(
                        1 for info in final_status.values() 
                        if info.get('statename') == 'RUNNING'
                    )
                    total_final = len(final_status)
                    rprint(f"✅ Final status: {running_final}/{total_final} processes running")
                    rprint("[dim]Use 'finetune logs' to view process logs[/dim]")
                    rprint("[dim]Use 'finetune status' to check process status[/dim]")
            except Exception:
                pass


def _follow_supervisor_logs(app_config: Config, manager: SupervisorManager):
    """Follow supervisor logs like docker logs -f."""
    log_file = Path(app_config.supervisor.logfile)
    
    rprint(f"\n[cyan]📋 Following supervisor logs from {log_file}[/cyan]")
    rprint("[dim]Press Ctrl+C to stop following logs[/dim]")
    
    if not log_file.exists():
        rprint("[yellow]⚠️  Supervisor log file doesn't exist yet, waiting...[/yellow]")
        # Wait a bit for the log file to be created
        for _ in range(10):
            if log_file.exists():
                break
            time.sleep(0.5)
        
        if not log_file.exists():
            rprint("[red]❌ Supervisor log file not found[/red]")
            return
    
    try:
        # Use tail -f to follow the log file
        try:
            # Try using tail command (Unix/Linux/macOS)
            process = subprocess.Popen(
                ['tail', '-f', str(log_file)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Setup signal handler for clean shutdown
            def signal_handler(sig, frame):
                process.terminate()
                rprint("\n[yellow]Log following stopped[/yellow]")
                raise typer.Exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            
            # Stream the output
            for line in iter(process.stdout.readline, ''):
                if line:
                    # Color code log levels
                    line = line.rstrip()
                    if 'INFO' in line:
                        rprint(f"[dim]{line}[/dim]")
                    elif 'WARN' in line:
                        rprint(f"[yellow]{line}[/yellow]")
                    elif 'CRIT' in line or 'ERROR' in line:
                        rprint(f"[red]{line}[/red]")
                    elif 'spawned:' in line:
                        rprint(f"[green]{line}[/green]")
                    elif 'exited:' in line:
                        rprint(f"[red]{line}[/red]")
                    elif 'gave up:' in line:
                        rprint(f"[red bold]{line}[/red bold]")
                    else:
                        rprint(line)
            
        except FileNotFoundError:
            # Fallback to Python implementation if tail is not available
            _python_tail_follow(log_file)
            
    except KeyboardInterrupt:
        rprint("\n[yellow]Log following stopped[/yellow]")
    except Exception as e:
        rprint(f"[red]Error following logs: {e}[/red]")


def _python_tail_follow(log_file: Path):
    """Python implementation of tail -f for systems without tail command."""
    try:
        with open(log_file, 'r') as f:
            # Go to end of file
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if line:
                    line = line.rstrip()
                    # Color code log levels
                    if 'INFO' in line:
                        rprint(f"[dim]{line}[/dim]")
                    elif 'WARN' in line:
                        rprint(f"[yellow]{line}[/yellow]")
                    elif 'CRIT' in line or 'ERROR' in line:
                        rprint(f"[red]{line}[/red]")
                    elif 'spawned:' in line:
                        rprint(f"[green]{line}[/green]")
                    elif 'exited:' in line:
                        rprint(f"[red]{line}[/red]")
                    elif 'gave up:' in line:
                        rprint(f"[red bold]{line}[/red bold]")
                    else:
                        rprint(line)
                else:
                    time.sleep(0.1)  # Wait for new content
                    
    except KeyboardInterrupt:
        rprint("\n[yellow]Log following stopped[/yellow]")
    except Exception as e:
        rprint(f"[red]Error following logs: {e}[/red]")
