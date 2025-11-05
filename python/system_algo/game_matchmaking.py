# Real-Time Skill-Based Matchmaking System

# You are building a matchmaking system for a competitive online game (like League of Legends, 
# Valorant, or Chess.com). The system must efficiently match players based on:

# 1. Skill rating (ELO/MMR) - players should be matched with similar skill
# 2. Wait time - longer waiting players get priority and wider skill range
# 3. Role preference - players may prefer specific roles (e.g., tank, support, damage)
# 4. Region - players should match within their region for lower latency
# 5. Party size - solo players, duos, full teams

# CONSTRAINTS:
# - Hundreds of thousands of concurrent players searching for matches
# - Matches must be found within reasonable time (<2 minutes preferred)
# - Skill difference should be minimized while balancing queue times
# - Memory bounded - can't store every historical match
# - Must support different game modes (ranked, casual, custom)

# REQUIREMENTS:

# 1. add_player(player_id: str, skill_rating: int, preferred_roles: List[str], 
#               region: str, party_ids: List[str], game_mode: str, timestamp: int)
#    - Add a player to the matchmaking queue
#    - Players can queue solo or as a party
#    - Timestamp tracks when they started searching

# 2. remove_player(player_id: str)
#    - Player cancels queue

# 3. find_match() -> Optional[Match]
#    - Attempt to find a suitable match
#    - Returns Match object with matched players or None
#    - Should be called periodically (e.g., every 100ms)
#    - Must balance skill fairness vs wait time

# 4. get_queue_stats(game_mode: str, region: str) -> dict
#    - Return statistics: average wait time, queue size, skill distribution
#    - Should be efficient (called frequently for monitoring)

# 5. adjust_matchmaking_parameters(max_skill_diff: int, wait_time_expansion_rate: float)
#    - Tune matchmaking algorithm dynamically
#    - max_skill_diff: base skill difference allowed
#    - wait_time_expansion_rate: how quickly to expand search range over time

# 6. get_player_wait_time(player_id: str) -> int
#    - Get how long a player has been waiting

# MATCH RULES:
# - Standard match: 2 teams of 5 players each
# - Skill rating difference within a team should be < 500 (tunable)
# - Total skill difference between teams should be < 200 (tunable)
# - Wait time priority: players waiting >60s get 2x search range
# - Role balance: try to satisfy role preferences when possible

# PERFORMANCE REQUIREMENTS:
# - add_player: O(log N) acceptable
# - find_match: should complete in <10ms for fairness
# - get_queue_stats: O(1) or O(log N)
# - Support 100K+ concurrent players in queue

# EXAMPLE USAGE:

# system = MatchmakingSystem()

# # Players join queue
# system.add_player("p1", skill_rating=1500, preferred_roles=["tank"], 
#                   region="NA", party_ids=[], game_mode="ranked", timestamp=1000)
# system.add_player("p2", skill_rating=1520, preferred_roles=["support"],
#                   region="NA", party_ids=[], game_mode="ranked", timestamp=1000)
# # ... add 8 more players with similar ratings ...

# # Periodically try to find matches
# match = system.find_match()
# if match:
#     print(f"Match found: Team1={match.team1} Team2={match.team2}")
#     print(f"Avg wait time: {match.avg_wait_time}s")
#     print(f"Skill difference: {match.skill_difference}")

# # Monitor queue
# stats = system.get_queue_stats("ranked", "NA")
# print(f"Queue size: {stats['queue_size']}")
# print(f"Avg wait: {stats['avg_wait_time']}s")

# CHALLENGE QUESTIONS:
# 1. How do you handle parties of different sizes (solo, duo, full team)?
# 2. How do you prevent the same players from being matched repeatedly?
# 3. How do you ensure match quality doesn't degrade too much for long-waiting players?
# 4. How do you handle peak vs off-peak hours (different queue sizes)?
# 5. How do you implement "backfill" - adding players to ongoing matches?
# 6. What data structures optimize both skill-based and time-based matching?

# YOUR TASK:
# Implement the MatchmakingSystem class with proper:
# - Thread safety (multiple threads calling add_player, find_match)
# - Memory efficiency (bounded memory usage)
# - Match quality (balance skill vs wait time)
# - Performance (handle high throughput)