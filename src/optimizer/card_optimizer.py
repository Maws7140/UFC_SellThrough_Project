# UFC Fight Card Optimizer
# Builds optimal fight card using greedy + 2-opt algorithm

import random
from typing import List, Dict, Set, Optional

# Simple class to hold fight info
class Fight:
    def __init__(self, fight_id, fighter1, fighter2, weight_class, is_title=False):
        self.fight_id = fight_id
        self.fighter1 = fighter1  # dict with name and stats
        self.fighter2 = fighter2
        self.weight_class = weight_class
        self.is_title = is_title
        self.score = 0.0  # we'll calculate this later
    
    def __repr__(self):
        title_str = " [TITLE]" if self.is_title else ""
        return f"{self.fighter1['name']} vs {self.fighter2['name']} ({self.weight_class}){title_str}"


class FightCard:
    # Holds a list of fights and some stats about the card
    
    def __init__(self, fights=None):
        self.fights = fights or []
        self.predicted_sellthrough = 0.0
    
    def add_fight(self, fight):
        self.fights.append(fight)
    
    def remove_fight(self, fight):
        self.fights.remove(fight)
    
    def get_all_fighters(self):
        # Returns set of all fighter names on the card
        fighters = set()
        for f in self.fights:
            fighters.add(f.fighter1['name'])
            fighters.add(f.fighter2['name'])
        return fighters
    
    def get_weight_classes(self):
        # Returns set of weight classes on the card
        return {f.weight_class for f in self.fights}
    
    def count_title_fights(self):
        return sum(1 for f in self.fights if f.is_title)
    
    def is_valid(self, min_fights=10, max_fights=14, min_weight_classes=3):
        # Check if card meets basic constraints
        # Right number of fights?
        if len(self.fights) < min_fights or len(self.fights) > max_fights:
            return False
        
        # Enough variety in weight classes?
        if len(self.get_weight_classes()) < min_weight_classes:
            return False
        
        # No fighter appears twice this would be weird
        fighters = []
        for f in self.fights:
            if f.fighter1['name'] in fighters or f.fighter2['name'] in fighters:
                return False
            fighters.append(f.fighter1['name'])
            fighters.append(f.fighter2['name'])
        
        return True


def score_fight(fight, current_card_fights):
    # Score a fight based on how much it would help attendance
    # Higher score = better for the card
    # Uses heuristics: win rate, experience, finish rate, title fights, competitiveness
    
    f1 = fight.fighter1
    f2 = fight.fighter2
    
    score = 0.0
    
    # Win rate matters - successful fighters have fans
    win_rate_1 = f1.get('win_rate', 0.5)
    win_rate_2 = f2.get('win_rate', 0.5)
    score += (win_rate_1 + win_rate_2) * 0.2
    
    # Experience bonus - established fighters have built up fanbases
    exp_1 = min(f1.get('total_fights', 0) / 20, 1.0)  # cap at 20 fights
    exp_2 = min(f2.get('total_fights', 0) / 20, 1.0)
    score += (exp_1 + exp_2) * 0.15
    
    # Finish rate - exciting fighters who get KOs/submissions
    finish_1 = f1.get('finish_rate', 0.3)
    finish_2 = f2.get('finish_rate', 0.3)
    score += (finish_1 + finish_2) * 0.15
    
    # Title fights are HUGE draws
    if fight.is_title:
        score += 0.3
    
    # Close matchups are more interesting uncertainty = excitement
    competitiveness = 1.0 - abs(win_rate_1 - win_rate_2)
    score += competitiveness * 0.1
    
    # Bonus for adding a new weight class variety is good
    current_weights = {f.weight_class for f in current_card_fights}
    if fight.weight_class not in current_weights:
        score += 0.1
    
    return score


def calculate_card_sellthrough(card):
    # Estimate sell-through based on total fight quality
    # Returns value between 0.5 and 1.0 50-100% sell-through
    if not card.fights:
        return 0.5
    
    # Sum up all fight scores
    total_score = sum(f.score for f in card.fights)
    
    # Base sell-through + bonus from fight quality
    # Most UFC events sell at least 70-80% of tickets
    base = 0.75
    bonus = min(total_score / 15, 0.25)  # cap the bonus at 25%
    
    # Extra bonus for title fights (they really boost attendance)
    title_bonus = card.count_title_fights() * 0.02
    
    predicted = base + bonus + title_bonus
    
    # Keep it in valid range
    return max(0.5, min(1.0, predicted))


def greedy_build_card(available_fights, min_fights=10, max_fights=14):
    # Build initial card using greedy selection
    # Score all fights, sort by score, add best ones that don't conflict
    
    print("Building card with greedy algorithm...")
    
    card = FightCard()
    used_fighters = set()
    
    # Score and sort all fights
    for fight in available_fights:
        fight.score = score_fight(fight, [])
    
    sorted_fights = sorted(available_fights, key=lambda f: f.score, reverse=True)
    
    # Greedily add best fights
    for fight in sorted_fights:
        # Check if fighters are already on card
        if fight.fighter1['name'] in used_fighters:
            continue
        if fight.fighter2['name'] in used_fighters:
            continue
        
        # Add the fight
        card.add_fight(fight)
        used_fighters.add(fight.fighter1['name'])
        used_fighters.add(fight.fighter2['name'])
        
        # Stop if we have enough fights
        if len(card.fights) >= max_fights:
            break
    
    card.predicted_sellthrough = calculate_card_sellthrough(card)
    
    print(f"  Built card with {len(card.fights)} fights")
    print(f"  Predicted sell-through: {card.predicted_sellthrough:.1%}")
    
    return card


def two_opt_improve(card, available_fights, max_iterations=100):
        # Try to improve card by swapping fights 2-opt local search
    # Remove a fight, try adding a different one, keep if it's better
    
    print("Improving card with 2-opt...")
    
    best_card = card
    best_score = card.predicted_sellthrough
    
    # Get fights not currently on card
    card_ids = {f.fight_id for f in card.fights}
    unused_fights = [f for f in available_fights if f.fight_id not in card_ids]
    
    improved = True
    iterations = 0
    
    while improved and iterations < max_iterations:
        improved = False
        iterations += 1
        
        # Try swapping each fight on the card
        for i, current_fight in enumerate(best_card.fights):
            # What fighters become available if we remove this fight?
            freed_fighters = {current_fight.fighter1['name'], current_fight.fighter2['name']}
            used_fighters = best_card.get_all_fighters() - freed_fighters
            
            # Try each unused fight
            for new_fight in unused_fights:
                # Would this cause a conflict?
                new_fighters = {new_fight.fighter1['name'], new_fight.fighter2['name']}
                if new_fighters & used_fighters:  # intersection = conflict
                    continue
                
                # Make the swap
                new_fights = best_card.fights.copy()
                new_fights[i] = new_fight
                new_fight.score = score_fight(new_fight, new_fights)
                
                new_card = FightCard(new_fights)
                
                # Check if valid
                if not new_card.is_valid():
                    continue
                
                # Calculate new score
                new_score = calculate_card_sellthrough(new_card)
                
                # Is it better?
                if new_score > best_score + 0.001:  # small threshold to avoid tiny changes
                    best_card = new_card
                    best_score = new_score
                    improved = True
                    print(f"  Iteration {iterations}: Found improvement! New score: {new_score:.3f}")
                    break
            
            if improved:
                break
    
    best_card.predicted_sellthrough = best_score
    print(f"  2-opt complete after {iterations} iterations")
    
    return best_card


def optimize_card(available_fights, venue_capacity=20000):
    # Main function: greedy build first, then 2-opt improvement
    
    print("=" * 50)
    print("UFC Fight Card Optimizer")
    print("=" * 50)
    print(f"Available fights: {len(available_fights)}")
    print(f"Venue capacity: {venue_capacity:,}")
    print()
    
    # Step 1: Greedy
    card = greedy_build_card(available_fights)
    
    # Step 2: 2-opt improvement
    card = two_opt_improve(card, available_fights)
    
    # Calculate predicted attendance
    predicted_attendance = int(card.predicted_sellthrough * venue_capacity)
    
    # Print results
    print()
    print("=" * 50)
    print("OPTIMIZED FIGHT CARD")
    print("=" * 50)
    print(f"Number of fights: {len(card.fights)}")
    print(f"Title fights: {card.count_title_fights()}")
    print(f"Weight classes: {len(card.get_weight_classes())}")
    print(f"Predicted sell-through: {card.predicted_sellthrough:.1%}")
    print(f"Predicted attendance: {predicted_attendance:,}")
    print()
    print("Fight Card:")
    print("-" * 50)
    
    # Sort by score to show best fights first
    for i, fight in enumerate(sorted(card.fights, key=lambda f: -f.score), 1):
        print(f"{i}. {fight}")
        print(f"   Score: {fight.score:.3f}")
    
    print("=" * 50)
    
    return card


# Demo with sample data
if __name__ == "__main__":
    
    # Create some sample fights to test with
    # In real use, this would come from our fighter data
    
    sample_fights = []
    
    # Main event - title fights
    sample_fights.append(Fight(
        "1",
        {"name": "Jon Jones", "win_rate": 0.96, "finish_rate": 0.7, "total_fights": 28},
        {"name": "Stipe Miocic", "win_rate": 0.8, "finish_rate": 0.6, "total_fights": 24},
        "Heavyweight",
        is_title=True
    ))
    
    sample_fights.append(Fight(
        "2",
        {"name": "Islam Makhachev", "win_rate": 0.95, "finish_rate": 0.6, "total_fights": 26},
        {"name": "Charles Oliveira", "win_rate": 0.8, "finish_rate": 0.85, "total_fights": 35},
        "Lightweight",
        is_title=True
    ))
    
    sample_fights.append(Fight(
        "3",
        {"name": "Alex Pereira", "win_rate": 0.85, "finish_rate": 0.9, "total_fights": 12},
        {"name": "Jamahal Hill", "win_rate": 0.8, "finish_rate": 0.7, "total_fights": 15},
        "Light Heavyweight"
    ))
    
    sample_fights.append(Fight(
        "4",
        {"name": "Sean O'Malley", "win_rate": 0.9, "finish_rate": 0.6, "total_fights": 18},
        {"name": "Merab Dvalishvili", "win_rate": 0.95, "finish_rate": 0.2, "total_fights": 17},
        "Bantamweight",
        is_title=True
    ))
    
    sample_fights.append(Fight(
        "5",
        {"name": "Max Holloway", "win_rate": 0.8, "finish_rate": 0.5, "total_fights": 30},
        {"name": "Ilia Topuria", "win_rate": 1.0, "finish_rate": 0.75, "total_fights": 15},
        "Featherweight"
    ))
    
    # Add some filler fights to have enough for a full card
    weight_classes = ["Welterweight", "Middleweight", "Flyweight", "Women's Strawweight", 
                      "Women's Bantamweight", "Featherweight"]
    
    for i, wc in enumerate(weight_classes):
        for j in range(2):  # 2 fights per weight class
            idx = 10 + i*2 + j
            
            # Random stats for variety
            f1_stats = {
                "name": f"Fighter_{idx}A",
                "win_rate": 0.5 + random.random() * 0.4,
                "finish_rate": 0.2 + random.random() * 0.5,
                "total_fights": random.randint(5, 20)
            }
            f2_stats = {
                "name": f"Fighter_{idx}B",
                "win_rate": 0.5 + random.random() * 0.4,
                "finish_rate": 0.2 + random.random() * 0.5,
                "total_fights": random.randint(5, 20)
            }
            
            sample_fights.append(Fight(str(idx), f1_stats, f2_stats, wc))
    
    # Run the optimizer
    random.seed(42)  # for reproducibility
    optimal_card = optimize_card(sample_fights, venue_capacity=20000)
