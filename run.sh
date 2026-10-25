#!/usr/bin/env bash
# ==============================================================================
# run.sh - Media Organizer Pipeline Unified Launcher & Tool Runner
# ==============================================================================
# Provides an interactive menu and direct CLI subcommands to run the
# Pipeline Planner and all utility scripts documented in the README.
# ==============================================================================

set -e

# Resolve script root directory
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

# Ensure python3 is available
if ! command -v python3 &>/dev/null; then
    echo "❌ Error: python3 is not installed or not in PATH."
    exit 1
fi

PYTHON="python3"

# Styling & Colors
BOLD="\033[1m"
DIM="\033[2m"
CYAN="\033[36m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
MAGENTA="\033[35m"
BLUE="\033[34m"
RESET="\033[0m"

# Print banner
print_banner() {
    echo -e "${CYAN}${BOLD}"
    echo "  ╔═══════════════════════════════════════════════════════════════════════════╗"
    echo "  ║                  📸 MEDIA ORGANIZER PIPELINE RUNNER                      ║"
    echo "  ║            Unified Gateway for Planner, Triage, and Utilities             ║"
    echo "  ╚═══════════════════════════════════════════════════════════════════════════╝"
    echo -e "${RESET}"
}

# Check volume mount status
check_volumes() {
    echo -e "${DIM}Checking external drive status:${RESET}"
    for vol in "/Volumes/DELL1TB02" "/Volumes/LaCie" "/Volumes/Extreme Pro"; do
        if [ -d "$vol" ]; then
            echo -e "  ${GREEN}✓ Mounted${RESET}   : ${BOLD}$vol${RESET}"
        else
            echo -e "  ${RED}✗ Offline${RESET}   : ${DIM}$vol${RESET}"
        fi
    done
    echo ""
}

# CLI Help
print_help() {
    print_banner
    echo -e "${BOLD}Usage:${RESET} ./run.sh [subcommand] [arguments...]"
    echo ""
    echo -e "${BOLD}Subcommands:${RESET}"
    echo -e "  ${GREEN}planner${RESET}            Launch the interactive Pipeline Planner (menu, batch mgmt, curation)"
    echo -e "  ${GREEN}refresh | bg${RESET}       Run the background database refresh service and metadata sync"
    echo -e "  ${GREEN}executor${RESET}           Execute planned batch operations (move, copy, organize)"
    echo ""
    echo -e "  ${CYAN}triage${RESET} [dir] [flags]  Run Intake Triage Gateway (--delete-duplicates, --ignore-duplicates, --dry-run)"
    echo -e "  ${CYAN}triage-skip-dup${RESET} [dir] Triage intake skipping duplicate copies (only route Review & New)"
    echo -e "  ${CYAN}triage-delete-dup${RESET} [dir] Triage intake and delete confirmed duplicates from source folder"
    echo -e "  ${CYAN}photos${RESET} [lib] [flags]  Triage Apple Photos Library (--ignore-duplicates, original filenames)"
    echo -e "  ${CYAN}reconcile${RESET} [--triage-id N] Audit/reconcile Live Photo companion MOV videos for a session"
    echo ""
    echo -e "  ${MAGENTA}review${RESET}             Interactive Folder Review (Ignore junk folders or Schedule for library)"
    echo -e "  ${MAGENTA}decisions${RESET}          List all recorded Ignored and Scheduled folder decisions"
    echo ""
    echo -e "  ${YELLOW}isolated${RESET} [--top N]   Report non-duplicate intake assets ranked by temporal uniqueness"
    echo -e "  ${YELLOW}triage-report${RESET} [id]   Intake deduplication summary report across all triage sessions"
    echo -e "  ${YELLOW}duplicates${RESET}         Master library annual asset distribution & duplicate cluster report"
    echo -e "  ${YELLOW}verify-duplicates${RESET}  Independent safety audit of Triage Duplicates against Master DB"
    echo ""
    echo -e "  ${BLUE}merge${RESET} [src] [dst]    Run the recursive Directory Merger and deduplication utility"
    echo -e "  ${BLUE}help${RESET}               Show this help message"
    echo ""
    echo -e "${DIM}Running ./run.sh without arguments opens the full interactive selector.${RESET}"
}

# Interactive Menu Loop
interactive_menu() {
    while true; do
        clear
        print_banner
        check_volumes

        echo -e "${BOLD}Select an action to perform:${RESET}"
        echo ""
        echo -e "  ${BOLD}${GREEN}── 🚀 Core Pipeline ──${RESET}"
        echo -e "  ${BOLD} 1)${RESET} ${GREEN}Interactive Pipeline Planner${RESET}        ${DIM}(scripts/pipeline_planner.py)${RESET}"
        echo -e "  ${BOLD} 2)${RESET} ${GREEN}Background Database Refresh${RESET}         ${DIM}(scripts/bg_copy_db_service.py)${RESET}"
        echo -e "  ${BOLD} 3)${RESET} ${GREEN}Execute Planned Batches${RESET}             ${DIM}(scripts/pipeline_executor.py)${RESET}"
        echo ""
        echo -e "  ${BOLD}${CYAN}── 🛡️ Intake & Triage Gateway ──${RESET}"
        echo -e "  ${BOLD} 4)${RESET} ${CYAN}Triage Intake (Default Folder)${RESET}      ${DIM}(/Volumes/LaCie/Storage/Priority 4/Media Preprocessing)${RESET}"
        echo -e "  ${BOLD} 5)${RESET} ${CYAN}Triage Intake (Dry-Run Simulation)${RESET}  ${DIM}(Previews classification without moving files)${RESET}"
        echo -e "  ${BOLD} 6)${RESET} ${CYAN}Triage Intake (Skip/Delete Duplicates)${RESET} ${DIM}(Option to skip or delete confirmed duplicates)${RESET}"
        echo -e "  ${BOLD} 7)${RESET} ${CYAN}Triage Custom Directory / Volume${RESET}    ${DIM}(Specify any directory path to triage)${RESET}"
        echo -e "  ${BOLD} 8)${RESET} ${CYAN}Triage Apple Photos Library${RESET}         ${DIM}(Extract original filenames, skip duplicate copies)${RESET}"
        echo -e "  ${BOLD} 9)${RESET} ${CYAN}Reconcile Live Photo MOVs${RESET}           ${DIM}(Audit companion video UUIDs against master library)${RESET}"
        echo ""
        echo -e "  ${BOLD}${MAGENTA}── 🧭 Folder Decision Flow & Curation ──${RESET}"
        echo -e "  ${BOLD}10)${RESET} ${MAGENTA}Interactive Folder Review${RESET}           ${DIM}(Mark folders as Ignored or Scheduled with re-eval)${RESET}"
        echo -e "  ${BOLD}11)${RESET} ${MAGENTA}List Recorded Folder Decisions${RESET}     ${DIM}(View all currently Ignored & Scheduled folders)${RESET}"
        echo ""
        echo -e "  ${BOLD}${YELLOW}── 📊 Reports & Analytics ──${RESET}"
        echo -e "  ${BOLD}12)${RESET} ${YELLOW}Temporal Isolation Report${RESET}          ${DIM}(Bubble up unique assets furthest from master library)${RESET}"
        echo -e "  ${BOLD}13)${RESET} ${YELLOW}Intake Deduplication Summary${RESET}       ${DIM}(View stats across all 41+ triage intake sessions)${RESET}"
        echo -e "  ${BOLD}14)${RESET} ${YELLOW}Master Library Duplicate Report${RESET}    ${DIM}(Yearly distribution and duplicate clusters)${RESET}"
        echo -e "  ${BOLD}15)${RESET} ${YELLOW}Verify Duplicates vs Master DB${RESET}     ${DIM}(Confirm DELL1TB02 Duplicates against Photos DB)${RESET}"
        echo ""
        echo -e "  ${BOLD}${BLUE}── 📁 File Consolidation ──${RESET}"
        echo -e "  ${BOLD}16)${RESET} ${BLUE}Merge Directories Utility${RESET}          ${DIM}(scripts/merge_directories.py)${RESET}"
        echo ""
        echo -e "  ${BOLD} q)${RESET} ${RED}Quit${RESET}"
        echo ""
        echo -n -e "${BOLD}👉 Enter choice [1-16, q]: ${RESET}"
        read -r choice

        case "$choice" in
            1)
                echo -e "\n${GREEN}🚀 Launching Interactive Pipeline Planner...${RESET}\n"
                $PYTHON scripts/pipeline_planner.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            2)
                echo -e "\n${GREEN}🔄 Running Background Database Refresh & Sync...${RESET}\n"
                $PYTHON scripts/bg_copy_db_service.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            3)
                echo -e "\n${GREEN}⚡ Launching Pipeline Batch Executor...${RESET}\n"
                $PYTHON scripts/pipeline_executor.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            4)
                echo -e "\n${CYAN}🛡️ Running Intake Triage on Default Folder...${RESET}\n"
                $PYTHON scripts/triage_media_intake.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            5)
                echo -e "\n${CYAN}🛡️ Running Intake Triage Simulation (--dry-run)...${RESET}\n"
                $PYTHON scripts/triage_media_intake.py --dry-run
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            6)
                echo -e "\n${CYAN}🛡️ Running Intake Triage on Default Folder (Duplicates Management)...${RESET}\n"
                echo -n "Delete confirmed duplicates from intake folder? [y/N]: "
                read -r del_ans
                if [[ "$del_ans" =~ ^[Yy]$ ]]; then
                    $PYTHON scripts/triage_media_intake.py --delete-duplicates
                else
                    $PYTHON scripts/triage_media_intake.py --ignore-duplicates
                fi
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            7)
                echo -e "\n${CYAN}📁 Triage Custom Directory${RESET}"
                echo -n "Enter directory path: "
                read -r target_p
                target_p="${target_p#[\'\"]}"
                target_p="${target_p%[\'\"]}"
                if [ -z "$target_p" ] || [ ! -d "$target_p" ]; then
                    echo -e "${RED}Invalid directory: $target_p${RESET}"
                    sleep 2
                    continue
                fi
                echo -n "Run in Dry-Run mode? [y/N]: "
                read -r dry_ans
                echo -n "Skip copying duplicates to DELL1TB02 (--ignore-duplicates)? [y/N]: "
                read -r dup_ans
                del_ans="n"
                if [[ "$dup_ans" =~ ^[Yy]$ ]]; then
                    echo -n "Delete confirmed duplicates from source intake folder? [y/N]: "
                    read -r del_ans
                fi
                cmd_args=("$target_p")
                if [[ "$dry_ans" =~ ^[Yy]$ ]]; then
                    cmd_args+=(--dry-run)
                fi
                if [[ "$del_ans" =~ ^[Yy]$ ]]; then
                    cmd_args+=(--delete-duplicates)
                elif [[ "$dup_ans" =~ ^[Yy]$ ]]; then
                    cmd_args+=(--ignore-duplicates)
                fi
                $PYTHON scripts/triage_media_intake.py "${cmd_args[@]}"
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            8)
                echo -e "\n${CYAN}🍏 Triage Apple Photos Library Package${RESET}"
                echo -n "Enter .photoslibrary path: "
                read -r lib_p
                lib_p="${lib_p#[\'\"]}"
                lib_p="${lib_p%[\'\"]}"
                if [ -z "$lib_p" ] || [ ! -d "$lib_p" ]; then
                    echo -e "${RED}Invalid library package: $lib_p${RESET}"
                    sleep 2
                    continue
                fi
                echo -n "Skip copying duplicates (--ignore-duplicates)? [Y/n]: "
                read -r dup_ans
                if [[ "$dup_ans" =~ ^[Nn]$ ]]; then
                    $PYTHON scripts/triage_photos_library.py "$lib_p"
                else
                    $PYTHON scripts/triage_photos_library.py "$lib_p" --ignore-duplicates
                fi
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            9)
                echo -e "\n${CYAN}🎬 Reconcile Triage Live Photo MOVs${RESET}"
                echo -n "Enter Triage Session ID (e.g. 37 or 41, or leave empty for all): "
                read -r rec_id
                if [ -n "$rec_id" ]; then
                    $PYTHON scripts/reconcile_triage_session_movs.py --triage-id "$rec_id"
                else
                    $PYTHON scripts/reconcile_triage_session_movs.py
                fi
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            10)
                echo -e "\n${MAGENTA}🧭 Launching Interactive Folder Review & Decision Flow...${RESET}\n"
                $PYTHON scripts/triage_folder_review.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            11)
                echo -e "\n${MAGENTA}📋 Listing Recorded Folder Decisions...${RESET}\n"
                $PYTHON scripts/triage_folder_review.py --list
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            12)
                echo -e "\n${YELLOW}⏳ Running Temporal Isolation Report...${RESET}\n"
                echo -n "Enter number of top assets to bubble up [default: 30]: "
                read -r top_count
                top_count="${top_count:-30}"
                $PYTHON scripts/report_triage_isolated_assets.py --top "$top_count"
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            13)
                echo -e "\n${YELLOW}📊 Running Intake Deduplication Summary Report...${RESET}\n"
                echo -n "Inspect specific session ID (or leave blank for all): "
                read -r sess_id
                if [ -n "$sess_id" ]; then
                    $PYTHON scripts/report_triage_deduplication.py --session "$sess_id"
                else
                    $PYTHON scripts/report_triage_deduplication.py
                fi
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            14)
                echo -e "\n${YELLOW}📈 Running Master Library Duplicate Report...${RESET}\n"
                $PYTHON scripts/report_library_duplicates.py
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            15)
                echo -e "\n${YELLOW}🛡️ Verify Duplicates Against Apple Photos Master DB${RESET}\n"
                echo -n "Enter specific Triage Session ID (or leave blank to verify ALL sessions): "
                read -r sess_id
                echo -n "Perform bit-for-bit disk comparisons against originals on disk? [y/N]: "
                read -r disk_ans
                extra_args=()
                if [ -n "$sess_id" ]; then
                    extra_args+=(--session "$sess_id")
                fi
                if [[ "$disk_ans" =~ ^[Yy]$ ]]; then
                    extra_args+=(--verify-disk)
                fi
                $PYTHON scripts/verify_triage_duplicates.py "${extra_args[@]}"
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            16)
                echo -e "\n${BLUE}📁 Merge Directories Utility${RESET}"
                echo -n "Enter Source directory: "
                read -r src_dir
                src_dir="${src_dir#[\'\"]}"
                src_dir="${src_dir%[\'\"]}"
                echo -n "Enter Destination directory: "
                read -r dst_dir
                dst_dir="${dst_dir#[\'\"]}"
                dst_dir="${dst_dir%[\'\"]}"
                if [ -z "$src_dir" ] || [ -z "$dst_dir" ]; then
                    echo -e "${RED}Source and Destination are required.${RESET}"
                    sleep 2
                    continue
                fi
                echo -n "Run in Dry-Run simulation mode? [Y/n]: "
                read -r dry_ans
                if [[ "$dry_ans" =~ ^[Nn]$ ]]; then
                    $PYTHON scripts/merge_directories.py "$src_dir" "$dst_dir"
                else
                    $PYTHON scripts/merge_directories.py "$src_dir" "$dst_dir" --dry-run
                fi
                echo -e "\n${DIM}Press Enter to return to menu...${RESET}"
                read -r
                ;;
            q|Q)
                echo -e "\n${GREEN}👋 Goodbye!${RESET}\n"
                exit 0
                ;;
            *)
                echo -e "${RED}⚠️  Invalid selection. Please enter a number between 1 and 16, or 'q'.${RESET}"
                sleep 1.5
                ;;
        esac
    done
}

# Subcommand Dispatcher
case "$1" in
    planner)
        shift
        exec $PYTHON scripts/pipeline_planner.py "$@"
        ;;
    refresh|bg)
        shift
        exec $PYTHON scripts/bg_copy_db_service.py "$@"
        ;;
    executor)
        shift
        exec $PYTHON scripts/pipeline_executor.py "$@"
        ;;
    triage)
        shift
        exec $PYTHON scripts/triage_media_intake.py "$@"
        ;;
    triage-skip-dup|triage-ignore-dup)
        shift
        exec $PYTHON scripts/triage_media_intake.py --ignore-duplicates "$@"
        ;;
    triage-delete-dup|triage-remove-dup)
        shift
        exec $PYTHON scripts/triage_media_intake.py --delete-duplicates "$@"
        ;;
    photos|triage-photos)
        shift
        exec $PYTHON scripts/triage_photos_library.py "$@"
        ;;
    reconcile)
        shift
        exec $PYTHON scripts/reconcile_triage_session_movs.py "$@"
        ;;
    review|folder-review)
        shift
        exec $PYTHON scripts/triage_folder_review.py "$@"
        ;;
    decisions|folder-decisions)
        shift
        exec $PYTHON scripts/triage_folder_review.py --list "$@"
        ;;
    isolated|uniqueness)
        shift
        exec $PYTHON scripts/report_triage_isolated_assets.py "$@"
        ;;
    triage-report|report)
        shift
        exec $PYTHON scripts/report_triage_deduplication.py "$@"
        ;;
    duplicates|library-report)
        shift
        exec $PYTHON scripts/report_library_duplicates.py "$@"
        ;;
    verify-duplicates|verify|confirm-duplicates)
        shift
        exec $PYTHON scripts/verify_triage_duplicates.py "$@"
        ;;
    merge)
        shift
        exec $PYTHON scripts/merge_directories.py "$@"
        ;;
    help|--help|-h)
        print_help
        exit 0
        ;;
    "")
        # No arguments provided -> run interactive menu
        interactive_menu
        ;;
    *)
        echo -e "${RED}Unknown subcommand: '$1'${RESET}"
        echo -e "Run ${BOLD}./run.sh help${RESET} for available commands or ${BOLD}./run.sh${RESET} for the interactive menu."
        exit 1
        ;;
esac
