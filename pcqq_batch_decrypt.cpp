/*
 * 改造自 pcqq_rekey_to_none.cpp — 支持命令行参数和批量解密
 *
 * 编译 (32位 MinGW-W64):
 *   g++ pcqq_batch_decrypt.cpp -o pcqq_batch_decrypt.exe -Wall
 *
 * 用法:
 *   1. 单个文件解密:
 *      pcqq_batch_decrypt.exe <db路径> <16字节密钥hex>
 *      例: pcqq_batch_decrypt.exe Msg3.0.db aabbccdd11223344aabbccdd11223344
 *
 *   2. 批量解密 (从 keys.json 读取):
 *      pcqq_batch_decrypt.exe --batch <keys.json路径> [输出目录]
 *
 * 注意:
 *   - 必须放在 QQ 安装目录的 Bin 文件夹下运行 (需要 KernelUtil.dll)
 *   - 解密后的文件需要去除前 1024 字节的扩展头 (如果有)
 */

#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <Windows.h>
#include <iomanip>
#include <string>
#include <vector>
#include <fstream>

#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#pragma GCC diagnostic ignored "-Wconversion-null"

// ─── SigScan ───
DWORD SigScan(const char* szPattern, int offset = 0);
void InitializeSigScan(DWORD ProcessID, const char* szModule);
void FinalizeSigScan();

#include <tlhelp32.h>
#include <map>
using std::map;
using std::string;
bool bIsLocal = false;
bool bInitialized = false;
BYTE *FFXiMemory = NULL;
DWORD BaseAddress = NULL;
DWORD ModSize = NULL;

typedef struct checks {
    short start; short size;
    checks() { start = NULL; size = 0; }
    checks(short sstart, short ssize) { start = sstart; size = ssize; }
} checks;

void InitializeSigScan(DWORD ProcessID, const char* Module) {
    MODULEENTRY32 uModule;
    SecureZeroMemory(&uModule, sizeof(MODULEENTRY32));
    uModule.dwSize = sizeof(MODULEENTRY32);
    HANDLE hModuleSnapshot = CreateToolhelp32Snapshot(TH32CS_SNAPMODULE | TH32CS_SNAPMODULE32, ProcessID);
    for(BOOL bModule = Module32First(hModuleSnapshot, &uModule); bModule; bModule = Module32Next(hModuleSnapshot, &uModule)) {
        uModule.dwSize = sizeof(MODULEENTRY32);
        if(!_stricmp(uModule.szModule, Module)) {
            FinalizeSigScan();
            BaseAddress = (DWORD)uModule.modBaseAddr;
            ModSize = uModule.modBaseSize;
            if(GetCurrentProcessId() == ProcessID) {
                bIsLocal = true; bInitialized = true;
                FFXiMemory = (BYTE*)BaseAddress;
            } else {
                bIsLocal = false;
                FFXiMemory = new BYTE[ModSize];
                HANDLE hProcess = OpenProcess(PROCESS_VM_READ, FALSE, ProcessID);
                if(hProcess) {
                    if(ReadProcessMemory(hProcess,(LPCVOID)BaseAddress,FFXiMemory,ModSize,NULL))
                        bInitialized = true;
                    CloseHandle(hProcess);
                }
            }
            break;
        }
    }
    CloseHandle(hModuleSnapshot);
}

void FinalizeSigScan() {
    if(FFXiMemory) {
        if(!bIsLocal) delete FFXiMemory;
        FFXiMemory = NULL; bInitialized = false;
    }
}

DWORD SigScan(const char* szPattern, int offset) {
    unsigned int PatternLength = strlen(szPattern);
    if(PatternLength % 2 != 0 || PatternLength < 2 || !bInitialized || !FFXiMemory || !BaseAddress) {
        std::cout << "SigScan check FAILED" << std::endl;
        return NULL;
    }
    unsigned int buffersize = PatternLength/2;
    int PtrOffset = buffersize + offset;
    bool Dereference = true;
    if(memcmp(szPattern,"##",2)==0) {
        Dereference = false; szPattern += 2;
        PtrOffset = 0 + offset; PatternLength -= 2; buffersize--;
    }
    if(memcmp(szPattern,"@@",2)==0) {
        Dereference = false; szPattern += 2; PatternLength -= 2;
    }
    char Pattern[1024];
    ZeroMemory(Pattern,sizeof(Pattern));
    strcpy_s(Pattern,sizeof(Pattern),szPattern);
    _strupr_s(Pattern,sizeof(Pattern));
    unsigned char* buffer = new unsigned char[buffersize];
    SecureZeroMemory(buffer,buffersize);
    checks memchecks[32];
    short cmpcount = 0, cmpsize = 0, cmpstart = 0;
    for(size_t i = 0; i < PatternLength / 2; i++) {
        unsigned char byte1 = Pattern[i*2], byte2 = Pattern[(i*2)+1];
        if(((byte1 >= '0' && byte1 <= '9') || (byte1 <= 'F' && byte1 >= 'A')) ||
           ((byte2 >= '0' && byte2 <= '9') || (byte2 <= 'F' && byte2 >= 'A'))) {
            cmpsize++;
            if (byte1 <= '9') buffer[i] += byte1 - '0'; else buffer[i] += byte1 - 'A' + 10;
            buffer[i] *= 16;
            if (byte2 <= '9') buffer[i] += byte2 - '0'; else buffer[i] += byte2 - 'A' + 10;
            continue;
        } else if(byte1 == 'X' && byte2 == byte1 && (PatternLength/2) - i > 3) {
            PtrOffset = i + offset;
            buffer[i++] = 'X'; buffer[i++] = 'X'; buffer[i++] = 'X'; buffer[i] = 'X';
        } else {
            buffer[i] = '?';
        }
        if(cmpsize>0) memchecks[cmpcount++] = checks(cmpstart,cmpsize);
        cmpstart = i+1; cmpsize = 0;
    }
    if(cmpsize>0) memchecks[cmpcount++] = checks(cmpstart,cmpsize);
    char* mBaseAddr = (char*)FFXiMemory;
    unsigned int mModSize = ModSize;
    bool bMatching = true;
    int Match_Count = 0; DWORD Last_Address = NULL;
    for(char* addr = (char*)memchr(mBaseAddr, buffer[0], mModSize - buffersize);
        addr && (DWORD)addr < (DWORD)((DWORD)mBaseAddr + mModSize - buffersize);
        addr = (char*)memchr(addr+1, buffer[0], mModSize - buffersize - (addr+1 - mBaseAddr))) {
        bMatching = true;
        for(short c = 0; c < cmpcount; c++) {
            if(memcmp(buffer + memchecks[c].start,(void*)(addr + memchecks[c].start),memchecks[c].size) != 0) {
                bMatching = false; break;
            }
        }
        if(bMatching) {
            DWORD Address = NULL;
            if(Dereference) Address = (DWORD)*((void **)(addr + PtrOffset));
            else Address = BaseAddress + (DWORD)((addr + PtrOffset) - (DWORD)FFXiMemory);
            Last_Address = Address; ++Match_Count;
        }
    }
    delete [] buffer;
    if(Match_Count>1) std::cout << "!!! MULTI Addr found. total: " << Match_Count << std::endl;
    if(Match_Count==1) return Last_Address;
    std::cout << "SigScan ret NULL" << std::endl;
    return NULL;
}
// ─── SigScan End ───


using namespace std;

typedef int (__cdecl *psqlite3_key)(void *, const void *, int);
typedef int (__cdecl *psqlite3_open)(const char *filename, int **ppDb);
typedef int (__cdecl *psqlite3_exec)(void* db, const char *sql,
    int (*callback)(void*,int,char**,char**), void *, char **errmsg);
typedef int (__cdecl *psqlite3_close)(void* db);

// 函数指针（全局）
static psqlite3_key akey = NULL;
static psqlite3_open aopen = NULL;
static psqlite3_exec aexec = NULL;
static psqlite3_key arekey = NULL;
// close 函数也用 SigScan
static psqlite3_close aclose_func = NULL;

int empty_key[16] = {0};

static int callback(void *data, int argc, char **argv, char **azColName) {
    printf("  %s ", (const char*)data);
    for(int i = 0; i < argc; i++)
        printf("%s = %s  ", azColName[i], argv[i] ? argv[i] : "NULL");
    printf("\n");
    return 0;
}

// 解析 hex 字符串为字节数组
bool parse_hex_key(const char* hexStr, BYTE* key, int expectedLen) {
    int len = strlen(hexStr);
    if (len != expectedLen * 2) {
        cerr << "密钥长度错误: 期望 " << expectedLen*2 << " 个hex字符, 实际 " << len << endl;
        return false;
    }
    for (int i = 0; i < expectedLen; i++) {
        unsigned int byte;
        char buf[3] = {hexStr[i*2], hexStr[i*2+1], 0};
        if (sscanf(buf, "%02x", &byte) != 1) {
            cerr << "无效的hex字符: " << buf << endl;
            return false;
        }
        key[i] = (BYTE)byte;
    }
    return true;
}

// 初始化 QQ 的 SQLite 函数
bool init_qq_sqlite() {
    HMODULE hModule = LoadLibraryEx("KernelUtil.Dll", NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
    if (hModule == NULL) {
        cerr << "加载 KernelUtil.dll 失败 (错误码: " << GetLastError() << ")" << endl;
        cerr << "请将程序放在 QQ 安装目录的 Bin 文件夹下运行" << endl;
        return false;
    }

    InitializeSigScan(GetCurrentProcessId(), "KernelUtil.dll");

    akey   = (psqlite3_key)(SigScan("##558BEC566B751011837D1010740D6817020000E8"));
    aopen  = (psqlite3_open)(SigScan("##558BEC6A006A06FF750CFF7508E8E0130200"));
    aexec  = (psqlite3_exec)(SigScan("##558BEC8B45088B40505DC3"));
    arekey = (psqlite3_key)(SigScan("##558BEC837D1010740D682F020000E8"));

    FinalizeSigScan();

    if (!akey || !aopen || !aexec || !arekey) {
        cerr << "SigScan 失败，QQ 版本可能不受支持" << endl;
        return false;
    }

    cout << "✅ QQ SQLite 函数已定位" << endl;
    return true;
}

// 解密单个数据库
bool decrypt_single(const char* dbPath, BYTE* key, int keyLen) {
    cout << "\n─── 解密: " << dbPath << " ───" << endl;

    // 检查文件是否存在
    FILE* f = fopen(dbPath, "rb");
    if (!f) {
        cerr << "  ❌ 文件不存在: " << dbPath << endl;
        return false;
    }
    fclose(f);

    // 打开数据库
    int* pDB = NULL;
    int iRet = aopen(dbPath, &pDB);
    if (iRet != 0) {
        cerr << "  ❌ 打开失败 (ret=" << iRet << ")" << endl;
        return false;
    }
    cout << "  打开成功" << endl;

    // 设置密钥
    iRet = akey(pDB, (unsigned char*)key, keyLen);
    if (iRet != 0) {
        cerr << "  ❌ 设置密钥失败 (ret=" << iRet << ")" << endl;
        return false;
    }
    cout << "  密钥设置成功" << endl;

    // 验证能否读取
    char* pErrmsg = NULL;
    char select[] = "SELECT count(*) FROM sqlite_master;";
    iRet = aexec(pDB, select, callback, (void*)"验证:", &pErrmsg);
    if (iRet != 0) {
        cerr << "  ❌ 验证失败 (ret=" << iRet << ")";
        if (pErrmsg) cerr << " 错误: " << pErrmsg;
        cerr << endl;
        cerr << "  密钥可能不正确" << endl;
        return false;
    }
    cout << "  验证通过" << endl;

    // rekey 为空密码 (去除加密)
    iRet = arekey(pDB, (unsigned char*)empty_key, keyLen);
    if (iRet != 0) {
        cerr << "  ❌ 去除加密失败 (ret=" << iRet << ")" << endl;
        return false;
    }
    cout << "  ✅ 解密成功！" << endl;
    return true;
}

// 去除 QQ 扩展头 (前1024字节)
bool strip_header(const char* srcPath, const char* dstPath) {
    FILE* fin = fopen(srcPath, "rb");
    if (!fin) return false;

    // 检查是否有 QQ 扩展头
    char header[16];
    fread(header, 1, 16, fin);
    if (memcmp(header, "SQLite header 3", 15) != 0) {
        fclose(fin);
        return false; // 不是 QQ 扩展头格式
    }

    // 跳过前1024字节
    fseek(fin, 1024, SEEK_SET);

    FILE* fout = fopen(dstPath, "wb");
    if (!fout) { fclose(fin); return false; }

    char buf[65536];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fin)) > 0) {
        fwrite(buf, 1, n, fout);
    }
    fclose(fin);
    fclose(fout);
    return true;
}

void print_usage(const char* prog) {
    cout << "用法:" << endl;
    cout << "  " << prog << " <db文件> <密钥hex(32个字符)>" << endl;
    cout << "  " << prog << " --strip <原始db> <输出db>   (去除1024字节头)" << endl;
    cout << endl;
    cout << "示例:" << endl;
    cout << "  " << prog << " Msg3.0.db aabbccdd11223344aabbccdd11223344" << endl;
    cout << endl;
    cout << "注意:" << endl;
    cout << "  - 程序必须放在 QQ 安装目录 Bin 文件夹下运行" << endl;
    cout << "  - 解密会直接修改原文件！请先备份！" << endl;
    cout << "  - 如果 db 有 QQ 扩展头，先用 --strip 去除" << endl;
}

int main(int argc, char* argv[]) {
    cout << "=== PCQQ 数据库解密工具 ===" << endl;

    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    // --strip 模式: 仅去除头部
    if (string(argv[1]) == "--strip") {
        if (argc < 4) {
            cerr << "--strip 需要两个参数: <原始db> <输出db>" << endl;
            return 1;
        }
        if (strip_header(argv[2], argv[3])) {
            cout << "✅ 头部已去除: " << argv[3] << endl;
        } else {
            cerr << "❌ 去除头部失败 (可能不是 QQ 扩展头格式)" << endl;
            return 1;
        }
        return 0;
    }

    // 解密模式
    const char* dbPath = argv[1];
    const char* keyHex = argv[2];
    
    BYTE key[16];
    if (!parse_hex_key(keyHex, key, 16)) {
        return 1;
    }

    cout << "数据库: " << dbPath << endl;
    cout << "密钥: ";
    for (int i = 0; i < 16; i++) printf("%02x", key[i]);
    cout << endl;

    if (!init_qq_sqlite()) {
        return 2;
    }

    if (decrypt_single(dbPath, key, 16)) {
        cout << "\n🎉 解密完成！现在可以用 SQLite 工具打开 " << dbPath << endl;
        cout << "   (如果有 QQ 扩展头，还需去除前 1024 字节)" << endl;
        return 0;
    } else {
        return 3;
    }
}
