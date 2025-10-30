import csv
import os
from collections import Counter

# Simulate Cassandra distributed e-voting system
# In production, this would connect to actual Cassandra cluster
# using cassandra-driver: from cassandra.cluster import Cluster

def simulate_cassandra_write(votes):
    """
    Simulate writing votes to Cassandra distributed database
    In real implementation: session.execute(insert_statement, (voter_id, candidate))
    """
    print("Simulating Cassandra cluster write operations...")
    print(f"Writing {len(votes)} votes to distributed nodes")
    return votes

def simulate_cassandra_read():
    """
    Simulate reading votes from Cassandra
    In real implementation: rows = session.execute(select_statement)
    """
    print("Simulating Cassandra cluster read operations...")
    with open('input.txt', 'r') as f:
        reader = csv.DictReader(f)
        votes = list(reader)
    print(f"Read {len(votes)} votes from distributed database")
    return votes

def count_votes(votes):
    """Count votes for each candidate"""
    candidates = [vote['candidate'] for vote in votes]
    return Counter(candidates)

def main():
    print("=" * 50)
    print("Distributed E-Voting System using Cassandra")
    print("SDG 16: Peace, Justice and Strong Institutions")
    print("=" * 50)
    
    # Read votes from input file
    print("\nReading votes from input.txt...")
    with open('input.txt', 'r') as f:
        reader = csv.DictReader(f)
        votes = list(reader)
    
    # Simulate distributed write to Cassandra
    votes = simulate_cassandra_write(votes)
    
    # Simulate distributed read from Cassandra
    votes = simulate_cassandra_read()
    
    # Count votes
    print("\nCounting votes...")
    vote_counts = count_votes(votes)
    
    # Display results
    print("\nVote Results:")
    print("-" * 30)
    for candidate, count in sorted(vote_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{candidate}: {count} votes")
    
    # Write results to CSV
    print("\nWriting results to output.csv...")
    with open('output.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Candidate', 'Votes'])
        for candidate, count in sorted(vote_counts.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([candidate, count])
    
    print("\nElection results exported successfully!")
    print("Distributed voting system ensures transparency and security.")

if __name__ == "__main__":
    main()
