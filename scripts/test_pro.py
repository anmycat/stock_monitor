from modules.auction_engine import get_professional_system

system = get_professional_system()
print("Instance OK")
print("Enabled:", system.is_enabled())

system.enable()
print("Enabled after enable:", system.is_enabled())

result = system.process({}, {
    'limit_up_count': 45,
    'auction_up_ratio': 0.6,
    'index_5d_change': 2.0
})
print("Process result:", result)
print("Regime:", system.get_regime())

result2 = system.process({}, {
    'limit_up_count': 5,
    'auction_up_ratio': 0.4,
    'index_5d_change': -3.0
})
print("Regime2:", system.get_regime())
