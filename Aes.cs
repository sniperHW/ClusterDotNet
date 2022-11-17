using System.Security.Cryptography;

namespace ClusterDotNet;

public class AES {

    static private Random rand = new Random(); 
    static private readonly int blocksize = 16;
    static private byte[] fixKey(byte[] key) 
    {
        if(key.Length > 32) {
            return new Span<byte>(key,0,32).ToArray();
        } else {
            int size = 0;
            if(key.Length < 16) {
                size = 16;
            } else if(key.Length < 24) {
                size = 24;
            } else {
                size = 32;
            }
            var padding = size - key.Length%size;
            using MemoryStream stream = new MemoryStream(new byte[size]);
            stream.Write(key);
            for(var i = 0;i < padding;i++) {
                stream.WriteByte((byte)padding);
            }
            return stream.ToArray();
        }
    } 
    static private byte[] paddingData(byte[] ciphertext,int blockSize) 
    {
        var paddingSize = blockSize - (ciphertext.Length + 4)%blockSize;
        using MemoryStream stream = new MemoryStream(new byte[ciphertext.Length + 4 + paddingSize]);
        stream.Write(BitConverter.GetBytes(Endian.Big(ciphertext.Length)));
        var padding = blockSize - ciphertext.Length%blockSize - 4;
        stream.Write(ciphertext);
        for(var i = 0;i < padding;i++) {
            stream.WriteByte((byte)padding);
        }
        return stream.ToArray();
    }

    static public byte[] CbcEncrypt(byte[] keybyte, byte[] plainbyte) 
    {
        keybyte = fixKey(keybyte);

        plainbyte = paddingData(plainbyte,blocksize);

        var aes = Aes.Create();

        using MemoryStream stream = new MemoryStream();

           
        for(var i = 0;i < blocksize;i++){
            stream.WriteByte((byte)rand.Next());
        }

        aes.Key = keybyte;
        aes.IV = stream.ToArray();
        aes.Padding = PaddingMode.None;
        aes.Mode = CipherMode.CBC;
        var _crypto = aes.CreateEncryptor(aes.Key, aes.IV);
        byte[] encrypted = _crypto.TransformFinalBlock(plainbyte, 0, plainbyte.Length);
        stream.Write(encrypted);        
        _crypto.Dispose();
        return stream.ToArray();
    }

    static public byte[]CbcDecrypt(byte[] keybyte, byte[] plainbyte)
    {

        keybyte = fixKey(keybyte);

        var aes = Aes.Create();
        aes.Key = keybyte;
        aes.IV  = new Span<byte>(plainbyte,0,blocksize).ToArray();
        aes.Padding = PaddingMode.None;
        aes.Mode = CipherMode.CBC;

        var _crypto = aes.CreateDecryptor(aes.Key, aes.IV);

        var encrypted = new Span<byte>(plainbyte,blocksize,plainbyte.Length-blocksize).ToArray();

        byte[] decrypted = _crypto.TransformFinalBlock(encrypted, 0, encrypted.Length);
        _crypto.Dispose();

        int payload =  Endian.Big(BitConverter.ToInt32(decrypted, 0));

        return new Span<byte>(decrypted,sizeof(int),payload).ToArray();
    }
}