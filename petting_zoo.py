from pettingzoo.classic import boxing_v1
import random

# Load environment
env = boxing_v1.env(render_mode="ansi")  # 'ansi' = no graphics

# Reset
env.reset()

# Battle!
for agent in env.agent_iter():
    observation, reward, termination, truncation, info = env.last()
    
    if termination or truncation:
        action = None  # No action if done
    else:
        action = env.action_space(agent).sample()  # Random move for now
    
    env.step(action)