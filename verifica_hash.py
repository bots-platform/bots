from passlib.context import CryptContext

# Pega aquí el hash copiado de tu users.json
hash_guardado = ""

# Escribe aquí la contraseña que quieres probar
password = ""

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
if pwd_context.verify(password, hash_guardado):
    print("¡La contraseña es correcta para este hash!")
else:
    print("La contraseña NO coincide con el hash.")